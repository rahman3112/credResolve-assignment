"""Progressive-mode reservation: agent + borrower + call row, all or nothing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from smartdialer.repo.agents import reserve_agent
from smartdialer.repo.borrowers import reserve_borrower
from smartdialer.repo.calls import insert_call
from smartdialer.repo.models import Agent, Borrower, Call
from smartdialer.states import CallState


@dataclass(frozen=True, slots=True)
class Reservation:
    call: Call
    agent: Agent | None  # None for predictive calls
    borrower: Borrower


class _NothingToDial(Exception):
    """Raised inside the transaction purely to roll it back."""


async def reserve_progressive_call(conn, campaign_id: int, now: datetime, lease_seconds: float) -> Reservation | None:
    """Reserve ONE agent and ONE borrower and create the call linking them (progressive
    mode: the agent is held BEFORE dialing, so agent-bound calls <= agents by construction).

    One transaction, so a crash can never leave an agent RESERVED with no call, or a call
    with no agent. If there is an agent but no dialable borrower, the agent reservation is
    rolled back with the rest.

    Deadlock-freedom: both claims use SKIP LOCKED, so no worker ever *waits* on a row another
    worker holds; the usual lock-order problem cannot arise.

    Returns None when there is no free agent or no dialable borrower.
    """
    lease = now + timedelta(seconds=lease_seconds)
    try:
        async with conn.transaction():
            agent = await reserve_agent(conn, campaign_id, now, lease)
            if agent is None:
                return None
            borrower = await reserve_borrower(conn, campaign_id, now)
            if borrower is None:
                raise _NothingToDial
            call = await insert_call(
                conn, campaign_id=campaign_id, borrower_id=borrower.id, agent_id=agent.id,
                state=CallState.RESERVED, attempt_no=borrower.attempts, now=now, lease_until=lease)
            return Reservation(call, agent, borrower)
    except _NothingToDial:
        return None


async def reserve_predictive_call(conn, campaign_id: int, now: datetime, lease_seconds: float) -> Reservation | None:
    """Predictive mode: claim ONE borrower and create an AGENTLESS call (state QUEUED).

    No agent is held. That is the whole point of predictive dialing (and its whole risk): the
    agent is reserved later, atomically, at the moment the borrower answers (callbacks.py). How
    many such calls may exist at once is the Safety Controller's decision, not this function's.

    The returned Reservation has agent=None."""
    lease = now + timedelta(seconds=lease_seconds)
    async with conn.transaction():
        borrower = await reserve_borrower(conn, campaign_id, now)
        if borrower is None:
            return None
        call = await insert_call(
            conn, campaign_id=campaign_id, borrower_id=borrower.id, agent_id=None,
            state=CallState.QUEUED, attempt_no=borrower.attempts, now=now, lease_until=lease)
        return Reservation(call, None, borrower)
