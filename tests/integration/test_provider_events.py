"""Duplicate / out-of-order / lost-correlation provider events (RULE 5)."""

import asyncio
from datetime import timedelta

import pytest

from smartdialer.repo.calls import get_call, insert_call
from smartdialer.repo.events import EventOutcome, ProviderEvent, ProviderEventType, apply_provider_event
from smartdialer.states import CallState

from .helpers import NOW, make_borrowers, make_campaign

pytestmark = pytest.mark.integration
T = ProviderEventType
O = EventOutcome


async def make_call(conn, *, state=CallState.INITIATED, provider_call_id="pc-1", provider="B"):
    cid = await make_campaign(conn, mode="PREDICTIVE")
    (b,) = await make_borrowers(conn, cid, 1)
    call = await insert_call(conn, campaign_id=cid, borrower_id=b.id, agent_id=None,
                             state=CallState.QUEUED, attempt_no=1, now=NOW,
                             lease_until=NOW + timedelta(seconds=60))
    await conn.execute("UPDATE calls SET state=$2, provider=$3, provider_call_id=$4 WHERE id=$1",
                       call.id, state.value, provider, provider_call_id)
    return call.id


def ev(t, seq=0, pcid="pc-1", provider="B", **kw):
    return ProviderEvent(provider, pcid, t, seq=seq, **kw)


async def feed(conn, events, at=NOW):
    return [(await apply_provider_event(conn, e, at)).outcome for e in events]


async def event_rows(conn, call_id):
    return await conn.fetch("SELECT * FROM provider_events WHERE call_id=$1 ORDER BY id", call_id)


async def test_answered_x3_then_completed_makes_exactly_two_transitions(conn):
    cid = await make_call(conn)
    outcomes = await feed(conn, [ev(T.ANSWERED)] * 3 + [ev(T.COMPLETED)])
    assert outcomes == [O.APPLIED, O.DUPLICATE, O.DUPLICATE, O.APPLIED]
    call = await get_call(conn, cid)
    assert call.state is CallState.COMPLETED
    assert call.version == 2  # initial 0 + ANSWERED + COMPLETED, nothing else
    rows = await event_rows(conn, cid)
    assert [(r["event_type"], r["duplicate_count"]) for r in rows] == [("ANSWERED", 2), ("COMPLETED", 0)]


async def test_completed_answered_ringing_ends_completed_and_keeps_late_facts(conn):
    cid = await make_call(conn)
    outcomes = await feed(conn, [ev(T.COMPLETED), ev(T.ANSWERED), ev(T.RINGING)])
    assert outcomes == [O.APPLIED, O.STALE, O.STALE]
    call = await get_call(conn, cid)
    assert call.state is CallState.COMPLETED
    assert call.version == 1 and call.answered_at is None  # state untouched by late events
    # ...but the late ANSWERED is stored, so answer-rate statistics can still count it.
    rows = await event_rows(conn, cid)
    assert {r["event_type"]: r["outcome"] for r in rows} == {
        "COMPLETED": "APPLIED", "ANSWERED": "STALE", "RINGING": "STALE"}


async def test_ringing_after_answered_is_stale(conn):
    cid = await make_call(conn)
    assert await feed(conn, [ev(T.ANSWERED), ev(T.RINGING)]) == [O.APPLIED, O.STALE]
    assert (await get_call(conn, cid)).state is CallState.ANSWERED


async def test_failed_call_is_not_resurrected(conn):
    cid = await make_call(conn, state=CallState.RINGING)
    out = await feed(conn, [ev(T.FAILED, reason="NO_ANSWER"), ev(T.ANSWERED), ev(T.COMPLETED)])
    assert out == [O.APPLIED, O.STALE, O.STALE]
    call = await get_call(conn, cid)
    assert call.state is CallState.FAILED and call.end_reason == "NO_ANSWER"
    assert call.lease_until is None  # terminal calls carry no lease


async def test_same_type_with_a_new_seq_is_a_new_row_but_not_a_new_transition(conn):
    cid = await make_call(conn)
    out = await feed(conn, [ev(T.ANSWERED, seq=1), ev(T.ANSWERED, seq=2)])
    assert out == [O.APPLIED, O.DUPLICATE]  # target == current -> DUPLICATE, stored once more
    assert (await get_call(conn, cid)).version == 1
    assert len(await event_rows(conn, cid)) == 2


async def test_forward_but_not_an_edge_is_illegal_and_ignored(conn):
    cid = await make_call(conn, state=CallState.QUEUED)
    # QUEUED -> ANSWERED skips INITIATED, which our own code always performs first.
    assert await feed(conn, [ev(T.ANSWERED)]) == [O.ILLEGAL]
    assert (await get_call(conn, cid)).state is CallState.QUEUED


async def test_every_permutation_of_a_normal_call_converges(pool):
    import itertools
    for n, perm in enumerate(itertools.permutations([T.RINGING, T.ANSWERED, T.COMPLETED])):
        async with pool.acquire() as conn:
            cid = await make_call(conn, provider_call_id=f"perm-{n}")
            await feed(conn, [ev(t, pcid=f"perm-{n}") for t in perm])
            assert (await get_call(conn, cid)).state is CallState.COMPLETED, perm


async def test_concurrent_duplicates_apply_once(pool):
    async with pool.acquire() as conn:
        cid = await make_call(conn)

    async def worker():
        async with pool.acquire() as conn:
            return (await apply_provider_event(conn, ev(T.ANSWERED), NOW)).outcome

    outcomes = await asyncio.gather(*[worker() for _ in range(20)])
    assert outcomes.count(O.APPLIED) == 1 and outcomes.count(O.DUPLICATE) == 19
    async with pool.acquire() as conn:
        assert (await get_call(conn, cid)).version == 1
        (row,) = await event_rows(conn, cid)
        assert row["duplicate_count"] == 19


async def test_event_and_transition_commit_or_roll_back_together(conn):
    """If the side effect inside the transaction fails, NEITHER the event row NOR the state
    change may survive: that is what 'one transaction' buys us."""
    cid = await make_call(conn)

    async def boom(c, before, after):
        raise RuntimeError("worker crashed mid-transaction")

    with pytest.raises(RuntimeError):
        await apply_provider_event(conn, ev(T.ANSWERED), NOW, on_applied=boom)
    assert (await get_call(conn, cid)).state is CallState.INITIATED
    assert await event_rows(conn, cid) == []
    # Redelivery after the "crash" is processed normally, not swallowed as a duplicate.
    assert await feed(conn, [ev(T.ANSWERED)]) == [O.APPLIED]


async def test_on_applied_sees_before_and_after(conn):
    cid = await make_call(conn)
    seen = []

    async def spy(c, before, after):
        seen.append((before.state, after.state))

    await apply_provider_event(conn, ev(T.ANSWERED), NOW, on_applied=spy)
    await apply_provider_event(conn, ev(T.ANSWERED), NOW, on_applied=spy)  # duplicate: no callback
    assert seen == [(CallState.INITIATED, CallState.ANSWERED)]


async def test_event_before_we_saved_provider_call_id_resolves_by_client_ref(conn):
    """The provider's first callback beat place_call()'s response (or we crashed before
    saving the id). The echoed client_ref finds the call and we adopt the provider id."""
    cid = await make_call(conn, provider=None, provider_call_id=None)
    res = await apply_provider_event(conn, ev(T.RINGING, pcid="new-id", client_ref=cid), NOW)
    assert res.outcome is O.APPLIED
    call = await get_call(conn, cid)
    assert (call.provider, call.provider_call_id, call.state) == ("B", "new-id", CallState.RINGING)
    # Later events no longer need the client_ref.
    assert await feed(conn, [ev(T.ANSWERED, pcid="new-id")]) == [O.APPLIED]


async def test_unknown_call_is_reported_and_not_stored(conn):
    res = await apply_provider_event(conn, ev(T.ANSWERED, pcid="ghost"), NOW)
    assert res.outcome is O.UNKNOWN_CALL and res.call is None
    assert await conn.fetchval("SELECT count(*) FROM provider_events") == 0


async def test_client_ref_of_another_provider_is_not_trusted(conn):
    cid = await make_call(conn, provider="A", provider_call_id="a-1")
    res = await apply_provider_event(conn, ev(T.ANSWERED, pcid="b-9", provider="B", client_ref=cid), NOW)
    assert res.outcome is O.UNKNOWN_CALL
