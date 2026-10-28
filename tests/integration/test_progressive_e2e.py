"""Progressive dialing end to end: reservation -> write-ahead intent -> provider -> events ->
agent and borrower consequences, on real PostgreSQL with a virtual clock."""

import dataclasses

import pytest

from smartdialer.providers.mock import PROFILE_A
from smartdialer.states import AgentState, CallState

from .harness import build

pytestmark = pytest.mark.integration
A = AgentState


async def test_happy_path_one_agent_one_borrower(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=20.0)
    seen = []
    for _ in range(200):
        await w.step()
        st = await w.agent_state(w.agent_ids[0])
        if not seen or seen[-1] != st:
            seen.append(st)
    # The agent walked the whole lifecycle, in order, and ended up free again.
    assert seen == [A.DIALING, A.CONNECTED, A.WRAP_UP, A.AVAILABLE]

    (call,) = await w.calls()
    assert call["state"] == "COMPLETED" and call["agent_id"] == w.agent_ids[0]
    assert call["initiated_at"] <= call["ringing_at"] <= call["answered_at"] <= call["connected_at"] <= call["ended_at"]
    assert call["provider"] == "A" and call["provider_call_id"] and call["lease_until"] is None
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"], b["last_outcome"]) == ("DONE", 1, "COMPLETED")
    d = await w.rows("SELECT * FROM safety_decisions")
    assert d and {r["verdict"] for r in d} == {"APPROVE"} and "one call per available agent" in d[0]["reason"]
    assert w.worker.safety.outstanding_permits == 0  # nothing left authorised


async def test_never_more_agent_bound_calls_than_agents(pool):
    """The headline progressive guarantee: 50 agents never carry more than 50 calls."""
    w = await build(pool, agents=50, borrowers=500, answer_rate=0.5, talk=60.0)
    peak = 0
    for _ in range(240):
        await w.step()
        inv = await w.assert_invariants()
        peak = max(peak, inv["bound_calls"])
    assert peak <= 50
    assert peak >= 40  # and we really did use the agents (this is not vacuous)
    total = await w.one("SELECT count(*) FROM calls")
    assert total > 100
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") == 0


async def test_no_answer_retries_with_backoff_then_exhausts(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=0.0, max_attempts=2)
    await w.run(250)
    calls = await w.calls()
    assert [(c["state"], c["end_reason"]) for c in calls] == [("FAILED", "NO_ANSWER")] * 2
    assert calls[1]["created_at"] - calls[0]["ended_at"] >= __import__("datetime").timedelta(seconds=30)  # backoff
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"]) == ("EXHAUSTED", 2)
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE  # a failed call never wraps up, never leaks


async def test_provider_error_releases_agent_and_does_not_burn_the_borrowers_attempts(pool):
    profile = dataclasses.replace(PROFILE_A, place_error_rate=1.0)
    w = await build(pool, agents=1, borrowers=1, profile=profile, breaker_kw={"min_samples": 10_000})
    await w.run(120)
    calls = await w.calls()
    assert len(calls) >= 5 and all(c["end_reason"] == "PROVIDER_ERROR" for c in calls[:-1])
    b = await w.borrower(w.borrower_ids[0])
    assert b["attempts"] <= 1 and b["status"] in ("PENDING", "RESERVED")  # never EXHAUSTED by a provider fault
    assert w.worker.stats.provider_errors >= 5


async def test_invalid_number_is_rejected_and_never_retried(pool):
    w = await build(pool, agents=2, borrowers=1, phones=["+0123"])
    await w.run(60)
    (c,) = await w.calls()
    assert (c["state"], c["end_reason"]) == ("FAILED", "REJECTED")
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "EXHAUSTED"
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE and await w.agent_state(w.agent_ids[1]) is A.AVAILABLE


# ---- ambiguous timeouts --------------------------------------------------------------

def timeout_profile(placed: float):
    return dataclasses.replace(PROFILE_A, place_error_rate=0.0, place_timeout_rate=1.0,
                               timeout_but_placed_rate=placed, timeout_seconds=10.0)


async def test_timeout_where_the_call_was_placed_is_adopted_not_redialed(pool):
    w = await build(pool, agents=1, borrowers=1, profile=timeout_profile(1.0), breaker_kw={"min_samples": 10_000})
    await w.run(200)
    assert w.worker.stats.timeouts_resolved_placed == 1
    assert w.raw.stats.placed == 1  # looked first, so the borrower's phone rang ONCE
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED"
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"


async def test_timeout_where_the_call_was_not_placed_fails_cleanly_and_refunds(pool):
    w = await build(pool, agents=1, borrowers=1, profile=timeout_profile(0.0), breaker_kw={"min_samples": 10_000})
    await w.run(20)
    first = (await w.calls())[0]
    assert (first["state"], first["end_reason"]) == ("FAILED", "PROVIDER_TIMEOUT")
    assert w.worker.stats.timeouts_not_placed >= 1 and w.raw.stats.placed == 0
    assert (await w.borrower(w.borrower_ids[0]))["attempts"] <= 1


async def test_unresolvable_timeout_is_left_for_the_reconciler_not_guessed(pool):
    """Total outage: place_call times out AND the follow-up status check times out. We cannot
    know whether the phone is ringing, so the call stays INITIATED, the agent stays DIALING and
    both keep a lease that the Phase-5 reconciler will pick up. Nothing is invented."""
    w = await build(pool, agents=1, borrowers=1, breaker_kw={"min_samples": 10_000})
    w.raw.start_outage()
    await w.run(30)
    (c,) = await w.calls()
    assert c["state"] == "INITIATED" and c["lease_until"] is not None
    assert await w.agent_state(w.agent_ids[0]) is A.DIALING
    assert w.worker.stats.timeouts_unresolved == 1


# ---- agent disappears ----------------------------------------------------------------

async def test_agent_vanishes_while_ringing_call_cancelled_and_provider_hung_up(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=600.0)
    await w.run(3)  # dialed, ringing at the provider
    (c,) = await w.calls()
    assert c["state"] in ("INITIATED", "RINGING")

    await w.worker.agents.agent_offline(w.agent_ids[0])
    (c,) = await w.calls()  # the database already reflects it: that is the reaction time
    assert (c["state"], c["end_reason"]) == ("CANCELLED", "AGENT_LOST")
    await w.sched.advance(3)  # background hangup reaches the provider
    assert await w.agent_state(w.agent_ids[0]) is A.OFFLINE
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"]) == ("PENDING", 0)  # not the borrower's fault: attempt refunded
    st = await w.sched.run(w.raw.get_call_status(client_ref=c["id"]))
    assert (st.state.value, st.reason, st.was_answered) == ("FAILED", "CANCELLED", False)  # provider told to stop
    await w.run(60)  # late events for the dead call are stale, nobody is dialed, nothing breaks
    assert len(await w.calls()) == 1
    await w.assert_invariants()


async def test_agent_vanishes_while_place_call_is_in_flight_compensating_hangup(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=600.0)
    await w.worker.tick()
    await w.sched.settle()  # _dial is now parked inside place_call's latency
    (c,) = await w.calls()
    assert c["state"] == "INITIATED" and c["provider_call_id"] is None

    await w.worker.agents.agent_offline(w.agent_ids[0])
    (c,) = await w.calls()
    assert c["state"] == "CANCELLED"
    await w.sched.advance(5)  # place_call returns: a call now exists that nobody owns...
    assert w.worker.stats.compensating_hangups == 1  # ...and we hang it up
    st = await w.sched.run(w.raw.get_call_status(client_ref=c["id"]))
    assert st.state.value == "FAILED" and st.reason == "CANCELLED"


async def test_agent_vanishes_between_reserve_and_dial(pool):
    from smartdialer.repo.reservation import reserve_progressive_call
    w = await build(pool, agents=1, borrowers=1)
    async with pool.acquire() as conn:
        res = await reserve_progressive_call(conn, w.cid, w.sched.now(), 60)
    assert res is not None
    await w.worker.agents.agent_offline(w.agent_ids[0])  # call RESERVED -> CANCELLED
    await w.worker.allocator._dial(res)  # the worker wakes up and finds it was taken over
    assert w.worker.stats.dial_skipped_taken_over == 1
    assert w.raw.stats.place_attempts == 0  # the provider was never contacted
    (c,) = await w.calls()
    assert (c["state"], c["end_reason"]) == ("CANCELLED", "AGENT_LOST")
    await w.assert_invariants()


async def test_agent_drops_mid_conversation(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=600.0)
    await w.run_until(lambda: _state_is(w, A.CONNECTED), 60)
    await w.worker.agents.agent_offline(w.agent_ids[0])
    await w.sched.advance(3)
    (c,) = await w.calls()
    assert (c["state"], c["end_reason"]) == ("FAILED", "AGENT_DROPPED")
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"]) == ("PENDING", 1)  # they did answer: counts, with backoff
    st = await w.sched.run(w.raw.get_call_status(client_ref=c["id"]))
    assert st.state.value == "COMPLETED"  # provider hung up the live call


async def test_answer_with_no_agent_to_bridge_is_recorded_as_an_abandon(pool):
    """Hook-level check of the abandon path: the borrower answers but the agent is no longer
    DIALING. (Cannot arise through normal flow, because agent_offline cancels first; this is
    the belt-and-braces branch.)"""
    from smartdialer.repo.events import ProviderEvent, ProviderEventType
    w = await build(pool, agents=1, borrowers=1)
    await w.worker.tick()
    await w.sched.settle()
    (c,) = await w.calls()
    await w.one("UPDATE agents SET state='AVAILABLE', lease_until=NULL WHERE id=$1", w.agent_ids[0])  # corrupt on purpose
    await w.worker.events.handle(ProviderEvent("A", "x-1", ProviderEventType.ANSWERED, seq=1, client_ref=c["id"]))
    (c,) = await w.calls()
    assert (c["state"], c["end_reason"]) == ("FAILED", "ABANDONED") and c["answered_at"] is not None
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE  # untouched
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "PENDING"


async def _state_is(w, state):
    return await w.agent_state(w.agent_ids[0]) is state


# ---- duplicate / out-of-order through the real processor -----------------------------

async def _dialing_call(w):
    """Reserve and write the intent for one call, WITHOUT contacting the provider, so the test
    can play the provider's part by hand."""
    from smartdialer.repo.reservation import reserve_progressive_call
    async with w.pool.acquire() as conn:
        res = await reserve_progressive_call(conn, w.cid, w.sched.now(), 60)
    assert await w.worker.allocator._intend(res)
    return res.call.id


async def test_duplicate_answered_events_cause_one_transition_per_state(pool):
    from smartdialer.repo.events import ProviderEvent, ProviderEventType as T
    w = await build(pool, agents=1, borrowers=1)
    cid = await _dialing_call(w)
    ev = lambda t, seq: ProviderEvent("A", "a-1", t, seq=seq, client_ref=cid)
    for e in [ev(T.ANSWERED, 2), ev(T.ANSWERED, 2), ev(T.ANSWERED, 2), ev(T.COMPLETED, 3)]:
        await w.worker.events.handle(e)
        await w.assert_invariants()
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED"
    # versions: INITIATED 1, provider id adopted 2, ANSWERED 3, CONNECTED 4, COMPLETED 5.
    # Three copies of ANSWERED + one COMPLETED added exactly three transitions, not four.
    assert c["version"] == 5
    assert await w.agent_state(w.agent_ids[0]) is A.WRAP_UP
    row = await w.rows_one("SELECT duplicate_count FROM provider_events WHERE event_type='ANSWERED'")
    assert row["duplicate_count"] == 2


async def test_completed_before_answered_and_ringing_releases_agent_without_wrapup(pool):
    from smartdialer.repo.events import ProviderEvent, ProviderEventType as T
    w = await build(pool, agents=1, borrowers=1)
    cid = await _dialing_call(w)
    for t, seq in [(T.COMPLETED, 3), (T.ANSWERED, 2), (T.RINGING, 1)]:
        await w.worker.events.handle(ProviderEvent("A", "a-1", t, seq=seq, client_ref=cid))
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED"
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE  # was never connected: no wrap-up
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"
    outcomes = {r["event_type"]: r["outcome"] for r in await w.rows("SELECT * FROM provider_events")}
    assert outcomes == {"COMPLETED": "APPLIED", "ANSWERED": "STALE", "RINGING": "STALE"}
    await w.assert_invariants()


async def test_pause_request_while_busy_is_applied_after_wrap_up(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=30.0, wrap_up=20)
    await w.run_until(lambda: _state_is(w, A.CONNECTED), 60)
    assert await w.worker.agents.agent_pause(w.agent_ids[0]) == "DEFERRED"
    assert await w.agent_state(w.agent_ids[0]) is A.CONNECTED  # the call is not stranded
    await w.run(200)
    assert await w.agent_state(w.agent_ids[0]) is A.PAUSED  # and afterwards: break, not AVAILABLE
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED" and len(await w.calls()) == 1  # a paused agent gets no more calls
    assert await w.worker.agents.agent_login(w.agent_ids[0])
    assert await w.worker.agents.agent_pause(w.agent_ids[0]) == "PAUSED"  # idle: immediately


# ---- multiple workers ----------------------------------------------------------------

async def test_three_workers_one_campaign_never_double_allocate(pool):
    w = await build(pool, agents=20, borrowers=200, answer_rate=0.5, talk=30.0, workers=3)
    peak = 0
    for _ in range(200):
        await w.step()
        inv = await w.assert_invariants()
        peak = max(peak, inv["bound_calls"])
    assert 15 <= peak <= 20
    # No borrower was ever on two live calls at once (the schema would have raised), and
    # no call exists without exactly one agent:
    assert await w.one("SELECT count(*) FROM calls WHERE agent_id IS NULL") == 0
    assert sum(wk.stats.reserved for wk in w.workers) == await w.one("SELECT count(*) FROM calls")


# ---- chaotic provider ----------------------------------------------------------------

async def test_provider_b_chaos_converges_with_no_abandons_and_no_leaks(pool):
    w = await build(pool, agents=15, borrowers=100, provider="B", answer_rate=0.5, talk=25.0,
                    breaker_kw={"min_samples": 10_000})
    await w.run(400)
    await w.quiesce(900)  # stop dialing, let every in-flight call (and late/duplicate event) land

    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") == 0
    states = {await w.agent_state(a) for a in w.agent_ids}
    assert states <= {A.AVAILABLE}  # every agent is back, none stuck DIALING/CONNECTED
    inv = await w.assert_invariants()
    assert inv["live_calls"] == 0 and inv["reserved_borrowers"] == 0
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") == 0
    assert w.raw.stats.event_copies_sent > w.raw.stats.events_emitted  # chaos really happened...
    assert (await w.one("SELECT sum(duplicate_count) FROM provider_events")) > 0  # ...and was deduped
    assert (await w.one("SELECT count(*) FROM calls WHERE state = 'COMPLETED'")) > 20
