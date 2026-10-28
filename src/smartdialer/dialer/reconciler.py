"""Reconciler: finds work whose owner has gone quiet and settles it by asking the provider.

What it handles (everything that a crash, a lost event or a provider outage can leave behind):

  * calls whose lease expired       -> ask the provider what is TRUE, then bring our records up
                                       to date by feeding that truth through the SAME event path
                                       real callbacks use (repo/events + dialer/callbacks), so
                                       rank rules, dedup, agent release and borrower requeue
                                       exist in exactly one place;
  * calls that never reached the provider (QUEUED / RESERVED, lease expired)
                                    -> the worker died before dialing: fail, release, requeue;
  * agents RESERVED/DIALING with no live call, borrowers RESERVED with no live call
                                    -> defensive repair (the atomic transactions should make
                                       these impossible; this makes "should" a checked claim).

Principles
  * NEVER GUESS. If the provider cannot be reached the call is left exactly as it is and
    looked at again later; absence of news is not news. The only place we conclude "this call
    does not exist" is the provider itself saying NOT_FOUND, and even then only if nothing
    touched the call since we looked (`expect_version`).
  * IDEMPOTENT AND SAFE TO RACE. Two reconcilers (or a reconciler and a live worker) may act
    on the same call. Every change is under the call's row lock, a CAS, or a deduplicated
    event, so the second actor finds the work already done. The advisory lock below only
    saves duplicate provider queries; correctness never depends on it (tests run without it).
  * Synthetic events use seq = -1 ("observed by the reconciler"), so re-observing the same
    fact is deduplicated and a later real event is merely a harmless duplicate or stale fact.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from smartdialer.dialer.callbacks import EventProcessor
from smartdialer.dialer.completion import fail_call
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.observed import OBSERVED_SEQ, events_for_status  # noqa: F401
from smartdialer.dialer.stats import DialerStats
from smartdialer.providers.base import ProviderCallState, ProviderCallStatus, ProviderError
from smartdialer.repo.agents import get_agent, transition_agent
from smartdialer.repo.borrowers import finish_borrower, get_borrower
from smartdialer.repo.calls import (
    expired_calls,
    get_call,
    renew_call_lease,
    set_provider_id,
)
from smartdialer.repo.campaigns import get_campaign
from smartdialer.repo.models import BorrowerStatus, Call
from smartdialer.scheduler import Scheduler
from smartdialer.states import NON_TERMINAL_CALL_STATES, TERMINAL_CALL_STATES, AgentState, CallState

LOCK_NAMESPACE = 5150  # first half of the advisory-lock key; second half is the campaign id


@dataclass
class ReconcileReport:
    skipped_locked: bool = False
    examined: int = 0
    never_dialed: int = 0  # QUEUED/RESERVED: failed as WORKER_CRASH
    not_found: int = 0  # provider has no such call: failed
    brought_up_to_date: int = 0  # provider was ahead of us: synthetic events applied
    still_live: int = 0  # provider agrees the call is in progress: lease renewed
    unreachable: int = 0  # provider could not be asked: left alone, retry later
    moved_on: int = 0  # changed under us (a live worker/event got there first): fine
    orphan_agents: int = 0
    orphan_borrowers: int = 0


class Reconciler:
    def __init__(self, pool, sched: Scheduler, provider, cfg: DialerConfig, stats: DialerStats | None = None,
                 processor: EventProcessor | None = None):
        self.pool, self.sched, self.provider, self.cfg = pool, sched, provider, cfg
        self.stats = stats or DialerStats()
        self.processor = processor or EventProcessor(pool, sched, provider, cfg, self.stats)

    # ---- entry points ----------------------------------------------------------------
    async def run_once(self, campaign_id: int) -> ReconcileReport:
        """One pass, guarded by a per-campaign advisory lock so that N workers each running a
        reconcile loop do the work once per interval, not N times. The lock is session-scoped, so
        it is released automatically if this process dies."""
        async with self.pool.acquire() as lock_conn:
            got = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1, $2)", LOCK_NAMESPACE, campaign_id)
            if not got:
                return ReconcileReport(skipped_locked=True)
            try:
                return await self.run_unlocked(campaign_id)
            finally:
                await lock_conn.execute("SELECT pg_advisory_unlock($1, $2)", LOCK_NAMESPACE, campaign_id)

    async def run_unlocked(self, campaign_id: int) -> ReconcileReport:
        """The pass itself. Public because it must be correct WITHOUT the lock (tested)."""
        report = ReconcileReport()
        now = self.sched.now()
        async with self.pool.acquire() as conn:
            due = await expired_calls(conn, campaign_id, now, self.cfg.reconcile_batch)
        report.examined = len(due)
        for chunk in _chunks(due, 20):  # bounded concurrency towards the provider
            await self.sched.gather(*(self._reconcile_call(c, report) for c in chunk))
        async with self.pool.acquire() as conn:
            await self._repair_orphans(conn, campaign_id, self.sched.now(), report)
        return report

    async def run(self, campaign_id: int, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.sched.sleep(self.cfg.reconcile_interval_seconds)
            await self.run_once(campaign_id)

    # ---- one call --------------------------------------------------------------------
    async def _reconcile_call(self, listed: Call, report: ReconcileReport) -> None:
        now = self.sched.now()
        async with self.pool.acquire() as conn:
            call = await get_call(conn, listed.id)
        if call is None or call.state in TERMINAL_CALL_STATES or (call.lease_until and call.lease_until > now):
            report.moved_on += 1  # finished or renewed since we listed it
            return

        if call.state in (CallState.QUEUED, CallState.RESERVED):
            # Never handed to the provider (RESERVED -> INITIATED is committed BEFORE place_call),
            # so there is nothing to ask. The worker died between reserving and dialing.
            async with self.pool.acquire() as conn:
                done = await fail_call(conn, call.id, "WORKER_CRASH", now, self.cfg, expect_version=call.version)
            report.never_dialed += done
            report.moved_on += not done
            return

        try:
            status = await self._ask(call)
        except ProviderError:
            report.unreachable += 1
            await self._look_again_later(call, now)
            return

        if status.state is ProviderCallState.NOT_FOUND:
            reason = "WORKER_CRASH" if call.state is CallState.INITIATED else "PROVIDER_LOST"
            async with self.pool.acquire() as conn:
                done = await fail_call(conn, call.id, reason, now, self.cfg, expect_version=call.version)
            report.not_found += done
            report.moved_on += not done
            return

        events = events_for_status(self.provider.name, call.id, status)
        for ev in events:
            await self.processor.handle(ev)
        async with self.pool.acquire() as conn:
            if status.provider_call_id and not call.provider_call_id:
                await set_provider_id(conn, call.id, self.provider.name, status.provider_call_id)
            after = await get_call(conn, call.id)
            if after.state in TERMINAL_CALL_STATES:
                report.brought_up_to_date += 1
            else:
                # Provider agrees the call is in progress (or is only slightly ahead): re-arm.
                report.brought_up_to_date += bool(events) and after.version != call.version
                report.still_live += 1
                if after.lease_until is None or after.lease_until <= now:
                    seconds = (self.cfg.connected_lease_seconds if after.state in (CallState.ANSWERED, CallState.CONNECTED)
                               else self.cfg.lease_seconds)
                    await renew_call_lease(conn, after, now + timedelta(seconds=seconds))

    async def _ask(self, call: Call) -> ProviderCallStatus:
        if call.provider_call_id:
            return await self.provider.get_call_status(provider_call_id=call.provider_call_id)
        return await self.provider.get_call_status(client_ref=call.id)  # crash before we saved the id

    async def _look_again_later(self, call: Call, now) -> None:
        """Provider unreachable. Do NOT conclude anything; just stop re-asking every pass."""
        async with self.pool.acquire() as conn:
            await renew_call_lease(conn, call, now + timedelta(seconds=self.cfg.reconcile_retry_seconds))

    # ---- orphans ---------------------------------------------------------------------
    async def _repair_orphans(self, conn, campaign_id: int, now, report: ReconcileReport) -> None:
        live = [s.value for s in NON_TERMINAL_CALL_STATES]
        agent_ids = await conn.fetchval(
            "SELECT coalesce(array_agg(a.id), '{}') FROM agents a WHERE a.campaign_id = $1 "
            "AND a.state IN ('RESERVED', 'DIALING') AND a.lease_until <= $2 "
            "AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.agent_id = a.id AND c.state = ANY($3::text[]))",
            campaign_id, now, live)
        for agent_id in agent_ids:
            async with conn.transaction():
                agent = await get_agent(conn, agent_id, for_update=True)
                still_orphan = not await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM calls WHERE agent_id = $1 AND state = ANY($2::text[]))", agent_id, live)
                if still_orphan and agent.state in (AgentState.RESERVED, AgentState.DIALING):
                    if await transition_agent(conn, agent, AgentState.AVAILABLE, now):
                        report.orphan_agents += 1

        borrower_ids = await conn.fetchval(
            "SELECT coalesce(array_agg(b.id), '{}') FROM borrowers b WHERE b.campaign_id = $1 AND b.status = 'RESERVED' "
            "AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.borrower_id = b.id AND c.state = ANY($2::text[]))",
            campaign_id, live)
        campaign = await get_campaign(conn, campaign_id)
        for borrower_id in borrower_ids:
            async with conn.transaction():
                b = await get_borrower(conn, borrower_id, for_update=True)
                still_orphan = not await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM calls WHERE borrower_id = $1 AND state = ANY($2::text[]))", borrower_id, live)
                if still_orphan and b.status is BorrowerStatus.RESERVED:
                    await finish_borrower(conn, b, now, done=False, outcome="ORPHAN_REPAIRED", refund_attempt=True,
                                          retry_delay=self.cfg.provider_retry_delay_seconds,
                                          max_attempts=campaign.max_attempts)
                    report.orphan_borrowers += 1


def _chunks(items: list, n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]
