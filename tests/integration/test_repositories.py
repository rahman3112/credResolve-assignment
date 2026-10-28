"""Single-threaded behaviour of the repository functions."""

from datetime import timedelta

import asyncpg
import pytest

from smartdialer.repo.agents import (
    count_by_state,
    create_agent,
    end_expired_wrap_ups,
    get_agent,
    reserve_agent,
    transition_agent,
)
from smartdialer.repo.borrowers import (
    backoff_seconds,
    finish_borrower,
    get_borrower,
    reserve_borrower,
)
from smartdialer.repo.calls import count_live_calls, get_call, insert_call, transition_call
from smartdialer.repo.models import BorrowerStatus
from smartdialer.repo.reservation import reserve_progressive_call
from smartdialer.states import AgentState, CallState, IllegalTransition

from .helpers import NOW, make_available_agents, make_borrowers, make_campaign

pytestmark = pytest.mark.integration
LEASE = NOW + timedelta(seconds=30)


def test_backoff_doubles_and_caps():
    assert [backoff_seconds(n) for n in (1, 2, 3, 4)] == [30, 60, 120, 240]
    assert backoff_seconds(30) == 3600


async def test_agent_lifecycle_and_lease_handling(conn):
    cid = await make_campaign(conn)
    (a,) = await make_available_agents(conn, cid, 1)
    r = await reserve_agent(conn, cid, NOW, LEASE)
    assert (r.state, r.lease_until, r.version) == (AgentState.RESERVED, LEASE, a.version + 1)
    d = await transition_agent(conn, r, AgentState.DIALING, NOW, lease_until=LEASE)
    c = await transition_agent(conn, d, AgentState.CONNECTED, NOW)
    assert c.lease_until is None  # leaving RESERVED/DIALING clears the lease
    w = await transition_agent(conn, c, AgentState.WRAP_UP, NOW, wrap_up_until=NOW + timedelta(seconds=30))
    # Not yet elapsed: nothing happens. Then it does.
    assert await end_expired_wrap_ups(conn, cid, NOW + timedelta(seconds=10)) == 0
    assert await end_expired_wrap_ups(conn, cid, NOW + timedelta(seconds=30)) == 1
    back = await get_agent(conn, w.id)
    assert back.state is AgentState.AVAILABLE and back.state_since == NOW + timedelta(seconds=30)


async def test_illegal_agent_transition_raises_before_touching_the_db(conn):
    cid = await make_campaign(conn)
    (a,) = await make_available_agents(conn, cid, 1)
    with pytest.raises(IllegalTransition):
        await transition_agent(conn, a, AgentState.CONNECTED, NOW)
    assert (await get_agent(conn, a.id)).version == a.version


async def test_lease_on_a_non_leasable_state_is_rejected_by_schema(conn):
    cid = await make_campaign(conn)
    (a,) = await make_available_agents(conn, cid, 1)
    with pytest.raises(ValueError):
        await transition_agent(conn, a, AgentState.PAUSED, NOW, lease_until=LEASE)


async def test_reserve_skips_agents_that_are_not_available(conn):
    cid = await make_campaign(conn)
    await create_agent(conn, cid, "offline", NOW)
    paused = await create_agent(conn, cid, "paused", NOW, AgentState.PAUSED)
    assert paused.state is AgentState.PAUSED
    assert await reserve_agent(conn, cid, NOW, LEASE) is None


async def test_reserve_is_scoped_to_the_campaign(conn):
    c1, c2 = await make_campaign(conn), await make_campaign(conn)
    await make_available_agents(conn, c1, 1)
    assert await reserve_agent(conn, c2, NOW, LEASE) is None


async def test_borrower_selection_order_backoff_and_exhaustion(conn):
    cid = await make_campaign(conn, max_attempts=2)
    low, high = await make_borrowers(conn, cid, 2)
    await conn.execute("UPDATE borrowers SET priority = 5 WHERE id = $1", high.id)

    b = await reserve_borrower(conn, cid, NOW)
    assert (b.id, b.attempts, b.status) == (high.id, 1, BorrowerStatus.RESERVED)  # priority first

    # Attempt 1 failed: back to PENDING, but NOT dialable until the backoff has passed.
    failed = await finish_borrower(conn, b, NOW, done=False, outcome="NO_ANSWER", max_attempts=2)
    assert failed.status is BorrowerStatus.PENDING and failed.next_attempt_at == NOW + timedelta(seconds=30)
    assert (await reserve_borrower(conn, cid, NOW)).id == low.id  # the other borrower goes first
    assert await reserve_borrower(conn, cid, NOW + timedelta(seconds=29)) is None
    again = await reserve_borrower(conn, cid, NOW + timedelta(seconds=30))
    assert (again.id, again.attempts) == (high.id, 2)

    # Attempt 2 (== max_attempts) failed: exhausted, never selected again.
    done = await finish_borrower(conn, again, NOW, done=False, outcome="NO_ANSWER", max_attempts=2)
    assert done.status is BorrowerStatus.EXHAUSTED
    assert await reserve_borrower(conn, cid, NOW + timedelta(days=1)) is None


async def test_finish_borrower_is_cas(conn):
    cid = await make_campaign(conn)
    await make_borrowers(conn, cid, 1)
    b = await reserve_borrower(conn, cid, NOW)
    assert await finish_borrower(conn, b, NOW, done=True, outcome="COMPLETED", max_attempts=3) is not None
    assert await finish_borrower(conn, b, NOW, done=False, outcome="x", max_attempts=3) is None  # stale snapshot
    assert (await get_borrower(conn, b.id)).status is BorrowerStatus.DONE


async def test_progressive_reservation_creates_linked_rows(conn):
    cid = await make_campaign(conn)
    await make_available_agents(conn, cid, 1)
    await make_borrowers(conn, cid, 1)
    res = await reserve_progressive_call(conn, cid, NOW, 30)
    assert res.call.state is CallState.RESERVED
    assert (res.call.agent_id, res.call.borrower_id) == (res.agent.id, res.borrower.id)
    assert res.call.attempt_no == 1 and res.call.lease_until == LEASE == res.agent.lease_until
    assert await count_live_calls(conn, cid) == 1


async def test_agent_without_borrower_is_rolled_back(conn):
    cid = await make_campaign(conn)
    (a,) = await make_available_agents(conn, cid, 1)
    assert await reserve_progressive_call(conn, cid, NOW, 30) is None
    assert (await get_agent(conn, a.id)).state is AgentState.AVAILABLE  # not left RESERVED
    assert (await count_by_state(conn, cid))[AgentState.RESERVED] == 0


async def test_insert_call_backstop_refuses_a_second_live_call_for_a_borrower(conn):
    cid = await make_campaign(conn)
    await make_available_agents(conn, cid, 2)
    (b,) = await make_borrowers(conn, cid, 1)
    kw = dict(campaign_id=cid, borrower_id=b.id, agent_id=None, state=CallState.QUEUED,
              attempt_no=1, now=NOW, lease_until=None)
    await insert_call(conn, **kw)
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_call(conn, **kw)


async def test_call_transitions_fill_timestamps_once_and_clear_lease_when_terminal(conn):
    cid = await make_campaign(conn)
    (b,) = await make_borrowers(conn, cid, 1)
    c = await insert_call(conn, campaign_id=cid, borrower_id=b.id, agent_id=None, state=CallState.QUEUED,
                          attempt_no=1, now=NOW, lease_until=LEASE)
    t1, t2, t3 = (NOW + timedelta(seconds=s) for s in (1, 2, 3))
    c = await transition_call(conn, c, CallState.INITIATED, t1, lease_until=LEASE, provider="A", provider_call_id="p1")
    c = await transition_call(conn, c, CallState.ANSWERED, t2, lease_until=LEASE)  # RINGING skipped
    c = await transition_call(conn, c, CallState.FAILED, t3, end_reason="ABANDONED")
    assert (c.initiated_at, c.ringing_at, c.answered_at, c.ended_at) == (t1, None, t2, t3)
    assert (c.provider, c.provider_call_id, c.lease_until, c.end_reason) == ("A", "p1", None, "ABANDONED")
    assert c.version == 3


async def test_stale_call_snapshot_loses_the_cas(conn):
    cid = await make_campaign(conn)
    (b,) = await make_borrowers(conn, cid, 1)
    c = await insert_call(conn, campaign_id=cid, borrower_id=b.id, agent_id=None, state=CallState.QUEUED,
                          attempt_no=1, now=NOW, lease_until=None)
    assert await transition_call(conn, c, CallState.INITIATED, NOW) is not None
    assert await transition_call(conn, c, CallState.CANCELLED, NOW) is None  # same snapshot again
    assert (await get_call(conn, c.id)).state is CallState.INITIATED
