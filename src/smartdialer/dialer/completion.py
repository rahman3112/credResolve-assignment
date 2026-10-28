"""What happens to the AGENT and the BORROWER when a call ends.

Every path that ends a call (provider event, provider failure at dial time, agent
disappearing, and later the reconciler) funnels through `release_after_call`, so there is
exactly one place that decides "does the agent go to wrap-up or straight back to AVAILABLE?"
and "does this borrower get retried, and when?".

Lock order, used everywhere to keep deadlocks impossible:  call -> agent -> borrower.
(`apply_provider_event` already holds the call lock when it invokes us.)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from smartdialer.dialer.config import DialerConfig
from smartdialer.providers.base import ProviderError
from smartdialer.repo.agents import get_agent, transition_agent
from smartdialer.repo.borrowers import finish_borrower, get_borrower
from smartdialer.repo.calls import agent_has_other_live_call, get_call, transition_call
from smartdialer.repo.campaigns import get_campaign
from smartdialer.repo.models import BorrowerStatus, Call
from smartdialer.states import AgentState, CallDecision, CallState, decide_call_transition

# Endings that are not the borrower's doing. The attempt is refunded and retried soon, so a
# provider outage or an agent logging off cannot use up a borrower's attempts.
NOT_BORROWERS_FAULT = frozenset({
    "PROVIDER_ERROR", "PROVIDER_TIMEOUT", "CIRCUIT_OPEN", "AGENT_LOST", "HUNG_UP_EXCESS",
    "WORKER_CRASH", "PROVIDER_LOST", "CANCELLED",  # reconciler / our own hangup
})


def borrower_args(call: Call, cfg: DialerConfig) -> tuple[str, dict]:
    """(outcome label, finish_borrower kwargs) for a terminal call."""
    reason = call.end_reason
    if call.state is CallState.COMPLETED:
        return "COMPLETED", {"done": True}
    if reason == "REJECTED":  # the provider says this number can never be dialed
        return "REJECTED", {"done": False, "status": BorrowerStatus.EXHAUSTED}
    if reason in NOT_BORROWERS_FAULT:
        return reason, {"done": False, "refund_attempt": True, "retry_delay": cfg.provider_retry_delay_seconds}
    # NO_ANSWER, BUSY, ABANDONED, AGENT_DROPPED, anything unknown: counts, exponential backoff.
    return reason or str(call.state), {"done": False}


async def release_after_call(conn, call: Call, now: datetime, cfg: DialerConfig) -> None:
    """`call` has just become terminal (same transaction). Free the agent, settle the borrower.

    Idempotent by construction: it only touches an agent still held by THIS call, and a
    borrower still RESERVED."""
    campaign = await get_campaign(conn, call.campaign_id)

    if call.agent_id is not None and not await agent_has_other_live_call(conn, call.agent_id, call.id):
        agent = await get_agent(conn, call.agent_id, for_update=True)
        if agent.state is AgentState.CONNECTED:
            until = now + timedelta(seconds=campaign.wrap_up_seconds)
            await must(transition_agent(conn, agent, AgentState.WRAP_UP, now, wrap_up_until=until), "agent->WRAP_UP")
        elif agent.state in (AgentState.RESERVED, AgentState.DIALING):
            # Never connected (no answer, failure, cancelled): no wrap-up needed.
            await must(transition_agent(conn, agent, AgentState.AVAILABLE, now), "agent->AVAILABLE")
        # OFFLINE / PAUSED / ...: the agent already left; nothing to give back.

    borrower = await get_borrower(conn, call.borrower_id, for_update=True)
    if borrower.status is BorrowerStatus.RESERVED:
        outcome, kw = borrower_args(call, cfg)
        await must(finish_borrower(conn, borrower, now, outcome=outcome, max_attempts=campaign.max_attempts,
                                    base_backoff=cfg.base_backoff_seconds, **kw), "borrower finish")


async def fail_call(conn, call_id: int, reason: str, now: datetime, cfg: DialerConfig, *,
                    expect_version: int | None = None) -> bool:
    """End a call as FAILED(reason) and release everything, if it is still ours to end.
    Returns False if it had already moved on (someone else got there first), which is fine.

    `expect_version`: used by the reconciler, whose decision ("the provider has no such call")
    was made on a snapshot. If anything touched the call since (a late event, a renewed lease),
    that decision may be stale, so we do nothing and look again next pass."""
    async with conn.transaction():
        call = await get_call(conn, call_id, for_update=True)
        if call is None or decide_call_transition(call.state, CallState.FAILED) is not CallDecision.APPLY:
            return False
        if expect_version is not None and call.version != expect_version:
            return False
        ended = await must(transition_call(conn, call, CallState.FAILED, now, end_reason=reason), "call->FAILED")
        await release_after_call(conn, ended, now, cfg)
        return True


async def hangup_quietly(provider, provider_call_id: str, stats) -> None:
    """Best-effort provider hangup. Failure is counted, not raised: the call is already
    terminal in our database, and the provider's own ring/talk limits are the backstop."""
    try:
        await provider.hangup(provider_call_id)
    except ProviderError:
        stats.hangup_failures += 1


async def must(awaitable, what: str):
    """A CAS that cannot lose because we hold the row locks; if it does, roll back loudly."""
    result = await awaitable
    if result is None:
        raise RuntimeError(f"unexpected CAS loss under lock: {what}")
    return result
