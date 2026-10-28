"""Call Allocator: turns a Permit into placed calls. The last stop before the provider.

Progressive flow for ONE call (every arrow is a separate, committed transaction unless noted):

  1. RESERVE   agent AVAILABLE->RESERVED, borrower PENDING->RESERVED, call row RESERVED.
               (one transaction: reserve_progressive_call)
  2. INTEND    call RESERVED->INITIATED, agent RESERVED->DIALING.  Written BEFORE the provider
               is contacted ("write-ahead"). Consequences:
                 * a crash after this point and before/during place_call leaves a call that
                   is INITIATED and *maybe* live at the provider; the reconciler resolves it
                   by asking the provider about our client_ref, never by guessing;
                 * no provider event can arrive for a call still in RESERVED.
  3. PLACE     provider.place_call(client_ref = call.id)   (outside any transaction: a DB
               transaction cannot span an external system).
  4. RECORD    save the provider's id on the call. Everything after that is driven by events.

How each step-3 outcome ends:
  ProviderRejected    bad number: call FAILED/REJECTED, borrower EXHAUSTED, agent released.
  CircuitOpenError    we never dialed: FAILED/CIRCUIT_OPEN, attempt refunded, agent released.
  ProviderError       the provider replied with an error, the call does NOT exist: FAILED/
                      PROVIDER_ERROR, attempt refunded, short retry delay, agent released.
  ProviderTimeout     AMBIGUOUS. Ask the provider about client_ref: not found -> as above;
                      found -> record it and carry on; can't even ask -> leave it INITIATED
                      for the reconciler. We never guess whether a borrower's phone is ringing.
"""

from __future__ import annotations

from datetime import timedelta

from smartdialer.clock import Clock
from smartdialer.dialer.completion import fail_call, hangup_quietly, must, release_after_call
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.observed import events_for_status
from smartdialer.dialer.safety import Permit, PermitKind, SafetyController
from smartdialer.dialer.stats import DialerStats
from smartdialer.providers.base import (
    CircuitOpenError,
    PlaceCallRequest,
    ProviderCallState,
    ProviderError,
    ProviderRejected,
    ProviderTimeout,
)
from smartdialer.repo.agents import get_agent, transition_agent
from smartdialer.repo.calls import get_call, set_provider_id, transition_call
from smartdialer.repo.reservation import Reservation, reserve_predictive_call, reserve_progressive_call
from smartdialer.scheduler import Scheduler
from smartdialer.states import TERMINAL_CALL_STATES, AgentState, CallState


class CallAllocator:
    def __init__(self, pool, sched: Scheduler, provider, safety: SafetyController,
                 cfg: DialerConfig, stats: DialerStats):
        self.pool, self.sched, self.provider = pool, sched, provider
        self.safety, self.cfg, self.stats = safety, cfg, stats
        self._verifying: set[int] = set()  # calls whose hangup is being verified right now
        self.events = None  # EventProcessor, set by the worker (verification feeds provider truth through it)
        self.clock: Clock = sched.clock

    async def execute(self, permit: Permit) -> int:
        """Start up to permit.n calls. Each call must be individually redeemed with the
        Safety Controller, so even a buggy loop here cannot exceed what was approved.
        Returns the number of calls started (reserved and handed to a dial task)."""
        reserve = reserve_progressive_call if permit.kind is PermitKind.PROGRESSIVE else reserve_predictive_call
        started = 0
        try:
            while True:
                now = self.clock.now()
                if not self.safety.redeem(permit, now):
                    break
                async with self.pool.acquire() as conn:
                    res = await reserve(conn, permit.campaign_id, now, self.cfg.lease_seconds)
                if res is None:  # no free agent or no dialable borrower right now
                    break
                self.stats.reserved += 1
                started += 1
                self.sched.spawn(self._dial(res), name=f"dial-{res.call.id}")
        finally:
            self.safety.void(permit)
        return started

    # ---- one call --------------------------------------------------------------------
    async def _dial(self, res: Reservation) -> None:
        call_id = res.call.id
        if not await self._intend(res):
            return
        try:
            placed = await self.provider.place_call(PlaceCallRequest(call_id, res.borrower.phone))
        except ProviderRejected:
            self.stats.rejected += 1
            await self._fail(call_id, "REJECTED")
        except CircuitOpenError:
            self.stats.circuit_open += 1
            await self._fail(call_id, "CIRCUIT_OPEN")
        except ProviderTimeout:
            await self._resolve_ambiguous(call_id)
        except ProviderError:
            self.stats.provider_errors += 1
            await self._fail(call_id, "PROVIDER_ERROR")
        else:
            self.stats.placed += 1
            await self._record(call_id, placed.provider_call_id)

    async def _intend(self, res: Reservation) -> bool:
        """Step 2. False if someone else (agent went offline, reconciler) took the call over."""
        now = self.clock.now()
        if res.agent is None:
            return await self._intend_agentless(res, now)
        async with self.pool.acquire() as conn, conn.transaction():
            call = await get_call(conn, res.call.id, for_update=True)  # lock order: call -> agent
            agent = await get_agent(conn, res.agent.id, for_update=True)
            if call.state is not CallState.RESERVED or agent.state is not AgentState.RESERVED \
                    or call.agent_id != agent.id:
                self.stats.dial_skipped_taken_over += 1
                return False
            lease = now + timedelta(seconds=self.cfg.lease_seconds)
            await must(transition_call(conn, call, CallState.INITIATED, now, lease_until=lease,
                                       provider=self.provider.name), "call->INITIATED")
            await must(transition_agent(conn, agent, AgentState.DIALING, now, lease_until=lease), "agent->DIALING")
            return True

    async def _intend_agentless(self, res: Reservation, now) -> bool:
        """Predictive step 2: no agent to move, only the call (QUEUED -> INITIATED)."""
        async with self.pool.acquire() as conn, conn.transaction():
            call = await get_call(conn, res.call.id, for_update=True)
            if call.state is not CallState.QUEUED:
                self.stats.dial_skipped_taken_over += 1
                return False
            await must(transition_call(conn, call, CallState.INITIATED, now,
                                       lease_until=now + timedelta(seconds=self.cfg.lease_seconds),
                                       provider=self.provider.name), "call->INITIATED")
            return True

    async def hang_up_excess(self, campaign_id: int, n: int) -> int:
        """Start hanging up to `n` still-ringing agentless calls, newest first. Ordered by the Safety
        Controller (Decision.hangup_n) when more calls are outstanding than the current free agents can
        safely cover (agents just dropped, rate surged). Returns how many were picked.

        Each pick is VERIFIED with the provider before anything destructive happens (_verify_then_cancel):
        our database may be behind (a slow provider's ANSWERED callback can be seconds late), and hanging
        up on someone who has already answered is dead air: an abandon in all but name. Verification takes
        a provider round trip, so this runs as background tasks; calls already being verified are skipped
        so the next tick does not pick them twice.

        NOT an abandon when it works: nobody answered, nobody was left waiting. Newest first because they
        have rung the least, so the borrower's experience is closest to "a missed call"."""
        async with self.pool.acquire() as conn:
            ids = [r["id"] for r in await conn.fetch(
                "SELECT id FROM calls WHERE campaign_id = $1 AND agent_id IS NULL AND state IN ('INITIATED', 'RINGING') "
                "AND NOT (id = ANY($3::bigint[])) ORDER BY created_at DESC, id DESC LIMIT $2",
                campaign_id, n, list(self._verifying))]
        for call_id in ids:
            self._verifying.add(call_id)
            self.sched.spawn(self._verify_then_cancel(call_id), name=f"hangup-{call_id}")
        return len(ids)

    async def _verify_then_cancel(self, call_id: int) -> None:
        try:
            async with self.pool.acquire() as conn:
                call = await get_call(conn, call_id)
            if call.state not in (CallState.INITIATED, CallState.RINGING) or call.agent_id is not None:
                return  # moved on while queued
            try:
                st = await self.provider.get_call_status(client_ref=call_id)
            except ProviderError:
                self.stats.hangups_unverified += 1  # cannot confirm it is only ringing: do NOT hang up (never guess)
                return
            if st.state in (ProviderCallState.ANSWERED, ProviderCallState.COMPLETED, ProviderCallState.FAILED):
                # Already past ringing at the provider. Bring our records up to date through the normal event
                # path instead (binds an agent, or records the abandon honestly) and do not cancel.
                self.stats.hangups_saved += 1
                for ev in events_for_status(self.provider.name, call_id, st):
                    await self.events.handle(ev)
                return
            await self._cancel_excess(call_id)
        finally:
            self._verifying.discard(call_id)

    async def _cancel_excess(self, call_id: int) -> None:
        now = self.clock.now()
        async with self.pool.acquire() as conn, conn.transaction():
            call = await get_call(conn, call_id, for_update=True)
            if call.state not in (CallState.INITIATED, CallState.RINGING) or call.agent_id is not None:
                return  # answered or finished while we were verifying: leave it alone
            ended = await must(transition_call(conn, call, CallState.CANCELLED, now, end_reason="HUNG_UP_EXCESS"),
                               "call->CANCELLED(excess)")
            await release_after_call(conn, ended, now, self.cfg)
        self.stats.hung_up_excess += 1
        if call.provider_call_id:  # else place_call is in flight: _record hangs up when it returns
            self.sched.spawn(hangup_quietly(self.provider, call.provider_call_id, self.stats))

    async def _fail(self, call_id: int, reason: str) -> None:
        async with self.pool.acquire() as conn:
            await fail_call(conn, call_id, reason, self.clock.now(), self.cfg)

    async def _record(self, call_id: int, provider_call_id: str) -> None:
        """Step 4. If the call was cancelled while place_call was in flight (the agent left),
        the provider is now ringing a borrower nobody will serve: hang up."""
        async with self.pool.acquire() as conn:
            await set_provider_id(conn, call_id, self.provider.name, provider_call_id)
            call = await get_call(conn, call_id)
        if call.state in TERMINAL_CALL_STATES:
            self.stats.compensating_hangups += 1
            await hangup_quietly(self.provider, provider_call_id, self.stats)

    async def _resolve_ambiguous(self, call_id: int) -> None:
        try:
            st = await self.provider.get_call_status(client_ref=call_id)
        except ProviderError:
            self.stats.timeouts_unresolved += 1  # reconciler's job; lease will expire
            return
        if st.state is ProviderCallState.NOT_FOUND:
            self.stats.timeouts_not_placed += 1
            await self._fail(call_id, "PROVIDER_TIMEOUT")
        else:
            self.stats.timeouts_resolved_placed += 1
            await self._record(call_id, st.provider_call_id)

