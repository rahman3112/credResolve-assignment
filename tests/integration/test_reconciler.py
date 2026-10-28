"""Crash recovery, lost events, provider outage, racing reconcilers, zombie workers."""

import asyncio
import dataclasses
import random

import pytest

from smartdialer.dialer.config import DialerConfig
from smartdialer.providers.base import PlaceCallRequest
from smartdialer.providers.mock import PROFILE_A
from smartdialer.repo.reservation import reserve_progressive_call
from smartdialer.states import AgentState, CallState

from .harness import build, dialing_call

pytestmark = pytest.mark.integration
A = AgentState
LEASE = 61  # just past DialerConfig.lease_seconds


def fixed_talk(w, seconds):
    """Every conversation lasts exactly `seconds` (min_talk is a floor, the mean is negligible)."""
    w.behavior.mean_talk_seconds, w.behavior.min_talk = 0.001, float(seconds)


async def call_row(w, call_id):
    return await w.rows_one("SELECT * FROM calls WHERE id = $1", call_id)


# ---- the brief's scenario: worker crashes after reserving / initiating ----------------

async def test_crash_after_reserve_before_dialing_is_recovered_without_calling_anyone(pool):
    w = await build(pool, agents=1, borrowers=1)
    async with pool.acquire() as conn:  # worker: agent reserved, borrower reserved, call RESERVED... dies
        res = await reserve_progressive_call(conn, w.cid, w.sched.now(), 60)
    assert res is not None
    assert (await w.reconcile()).examined == 0  # lease still running: leave the worker alone
    await w.assert_invariants()

    await w.sched.advance(LEASE)
    rep = await w.reconcile()
    assert (rep.examined, rep.never_dialed) == (1, 1)
    c = await call_row(w, res.call.id)
    assert (c["state"], c["end_reason"]) == ("FAILED", "WORKER_CRASH")
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE  # agent released
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"]) == ("PENDING", 0)  # requeued, attempt refunded
    assert w.raw.stats.place_attempts == 0  # reconciling a never-dialed call needs no provider at all

    await w.run(150)  # the system carries on: the borrower is dialed and the call completes
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE" and w.raw.stats.placed == 1


async def test_crash_after_initiated_before_place_call_provider_says_not_found(pool):
    w = await build(pool, agents=1, borrowers=1)
    call_id = await dialing_call(w)  # INITIATED + agent DIALING, then the worker dies
    await w.sched.advance(LEASE)
    rep = await w.reconcile()
    assert rep.not_found == 1 and rep.unreachable == 0
    c = await call_row(w, call_id)
    assert (c["state"], c["end_reason"]) == ("FAILED", "WORKER_CRASH")
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE
    assert (await w.borrower(w.borrower_ids[0]))["attempts"] == 0
    await w.run(150)
    assert w.raw.stats.placed == 1  # dialed exactly once overall: the crashed attempt never rang anyone
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"


async def test_crash_after_place_call_events_lost_reconciler_adopts_and_finishes_without_redialing(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0, talk=20.0)
    call_id = await dialing_call(w)
    w.raw.set_event_handler(None)  # nobody is listening: the worker that would have handled events is dead
    await w.sched.run(w.raw.place_call(PlaceCallRequest(call_id, "+15550000000")))  # the provider HAS the call
    await w.sched.advance(300)  # it rang, was answered, the talk finished: we saw none of it
    c = await call_row(w, call_id)
    assert c["state"] == "INITIATED" and c["provider_call_id"] is None  # our records are 5 minutes stale

    rep = await w.reconcile()
    assert rep.brought_up_to_date == 1 and rep.not_found == 0
    c = await call_row(w, call_id)
    assert c["state"] == "COMPLETED" and c["provider_call_id"] == "A-1"  # id adopted via client_ref
    assert c["answered_at"] is not None and c["connected_at"] is not None  # the conversation is on record
    assert await w.agent_state(w.agent_ids[0]) is A.WRAP_UP  # the agent really did talk: wrap-up, then free
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"
    assert w.raw.stats.placed == 1  # never redialed: it knew the truth instead of guessing

    again = await w.reconcile()  # idempotent
    assert again.examined == 0
    await w.run(40)
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE and len(await w.calls()) == 1


async def test_crash_mid_conversation_reconciler_reattaches_then_notices_the_end(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0)
    fixed_talk(w, 150)
    call_id = await dialing_call(w)
    w.raw.set_event_handler(None)
    await w.sched.run(w.raw.place_call(PlaceCallRequest(call_id, "+15550000000")))
    await w.sched.advance(LEASE)  # lease expires while the call is up (answered ~10-25s, talk ~100s)
    rep = await w.reconcile()
    c = await call_row(w, call_id)
    assert c["state"] == "CONNECTED" and await w.agent_state(w.agent_ids[0]) is A.CONNECTED  # re-attached, live
    assert rep.still_live == 1 and c["lease_until"] > w.sched.now()

    await w.sched.advance(200)  # the call ends at the provider; nobody tells us
    assert (await call_row(w, call_id))["state"] == "CONNECTED"
    assert (await w.reconcile()).examined >= 1  # connected lease (180s) has expired by now
    assert (await call_row(w, call_id))["state"] == "COMPLETED"
    assert await w.agent_state(w.agent_ids[0]) is A.WRAP_UP


# ---- worker crashes right after ANSWERED (the brief's wording) ------------------------

def crash_on_first_answered(w):
    """Make the event handler die inside its transaction the first time it sees ANSWERED."""
    real, seen = w.worker.events._on_applied, {"n": 0}

    async def crashing(conn, before, after, now, post):
        if after.state is CallState.ANSWERED and seen["n"] == 0:
            seen["n"] = 1
            raise RuntimeError("worker crashed right after ANSWERED")
        await real(conn, before, after, now, post)

    w.worker.events._on_applied = crashing
    return seen


async def test_worker_crash_on_answered_then_completed_arrives_state_is_still_sensible(pool):
    """ANSWERED handling crashes (its transaction rolls back; this provider never redelivers).
    COMPLETED then arrives. The call must end COMPLETED, the agent free, the borrower DONE."""
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0)
    fixed_talk(w, 30)
    seen = crash_on_first_answered(w)
    await w.run(200)
    assert seen["n"] == 1 and any("crashed right after ANSWERED" in str(e) for e in w.sched.background_errors)
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED"
    assert c["answered_at"] is None  # the one thing lost: the ANSWERED fact (it rolled back with the crash)
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"
    assert await w.one("SELECT count(*) FROM provider_events WHERE event_type = 'ANSWERED'") == 0
    # the crashed event left no half-applied trace behind:
    await w.assert_invariants()


async def test_worker_crash_on_answered_while_talking_reconciler_recovers_it(pool):
    w = await build(pool, agents=1, borrowers=1, answer_rate=1.0)
    fixed_talk(w, 300)
    crash_on_first_answered(w)
    await w.run(70)  # ANSWERED crashed; call is RINGING in our books but talking at the provider
    (c,) = await w.calls()
    assert c["state"] == "RINGING" and await w.agent_state(w.agent_ids[0]) is A.DIALING
    await w.reconcile()
    (c,) = await w.calls()
    assert c["state"] == "CONNECTED" and await w.agent_state(w.agent_ids[0]) is A.CONNECTED

    await w.run(500)  # the real COMPLETED event arrives later and is handled normally
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED" and len(await w.calls()) == 1
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE


async def test_events_are_served_by_any_worker_because_all_state_is_in_postgres(pool):
    w = await build(pool, agents=1, borrowers=1, workers=2, answer_rate=1.0, talk=20.0)
    await w.worker.tick()  # worker 1 dials...
    w.raw.set_event_handler(w.workers[1].events.handle)  # ...and then "dies"; worker 2 serves the callbacks
    await w.sched.advance(200)
    (c,) = await w.calls()
    assert c["state"] == "COMPLETED" and await w.agent_state(w.agent_ids[0]) is A.WRAP_UP


# ---- provider outage with calls in flight --------------------------------------------

async def test_outage_with_calls_in_flight_nothing_is_guessed_and_all_is_settled_after(pool):
    w = await build(pool, agents=10, borrowers=10, answer_rate=0.7, talk=60.0)
    await w.run(5)  # ten calls are up and ringing
    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") == 10
    async with pool.acquire() as conn:
        await conn.execute("UPDATE campaigns SET status = 'PAUSED' WHERE id = $1", w.cid)  # no new dials
    w.raw.start_outage()
    await w.run(400)  # calls answer, talk and end INSIDE the provider; every callback is dropped
    assert w.raw.stats.events_dropped_in_outage > 0

    live_before = await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')")
    rep = await w.reconcile()
    assert rep.unreachable == live_before and rep.unreachable >= 8  # asked, could not be answered, concluded NOTHING
    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") == live_before
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason IS NOT NULL") == 0  # no invented outcomes
    await w.assert_invariants()

    w.raw.end_outage()
    await w.sched.advance(40)  # past the "look again later" back-off
    rep = await w.reconcile()
    assert rep.unreachable == 0
    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") == 0
    reasons = {r["end_reason"] for r in await w.rows("SELECT end_reason FROM calls")}
    assert reasons <= {None, "NO_ANSWER", "BUSY"}  # truth from the provider, nothing else
    assert await w.one("SELECT count(*) FROM calls WHERE state = 'COMPLETED'") >= 4
    await w.run(60)  # wrap-ups elapse
    assert {await w.agent_state(a) for a in w.agent_ids} == {A.AVAILABLE}
    assert await w.one("SELECT count(*) FROM borrowers WHERE status = 'RESERVED'") == 0


async def test_stuck_timeout_from_phase_4_is_resolved_once_the_provider_is_back(pool):
    w = await build(pool, agents=1, borrowers=1, breaker_kw={"min_samples": 10_000})
    w.raw.start_outage()
    await w.run(30)  # place_call timed out AND the status check failed: call stays INITIATED
    (c,) = await w.calls()
    assert c["state"] == "INITIATED" and await w.agent_state(w.agent_ids[0]) is A.DIALING
    w.raw.end_outage()
    await w.sched.advance(40)
    rep = await w.reconcile()
    assert rep.not_found == 1  # the provider never got it: now we KNOW, so the call can be failed
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE
    await w.run(200)
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE" and w.raw.stats.placed == 1


# ---- concurrency and idempotency -----------------------------------------------------

async def _stuck_calls(w, n):
    ids = []
    w.raw.set_event_handler(None)
    for i in range(n):
        call_id = await dialing_call(w)
        await w.sched.run(w.raw.place_call(PlaceCallRequest(call_id, f"+1555000{i:04d}")))
        ids.append(call_id)
    await w.sched.advance(300)
    return ids


async def test_two_reconcilers_racing_without_the_lock_still_apply_everything_exactly_once(pool):
    w = await build(pool, agents=5, borrowers=5, answer_rate=1.0, talk=20.0)
    ids = await _stuck_calls(w, 5)
    other = type(w.reconciler)(pool, w.sched, w.provider, w.worker.cfg, processor=w.worker.events)

    async def both():
        return await w.sched.gather(w.reconciler.run_unlocked(w.cid), other.run_unlocked(w.cid))

    r1, r2 = await w.sched.run(both())
    assert r1.examined == r2.examined == 5  # both saw the same five expired calls...
    for i in ids:
        assert (await call_row(w, i))["state"] == "COMPLETED"
    assert {r["attempts"] for r in await w.rows("SELECT attempts FROM borrowers")} == {1}  # nobody double-refunded
    assert {r["status"] for r in await w.rows("SELECT status FROM borrowers")} == {"DONE"}
    assert {await w.agent_state(a) for a in w.agent_ids} == {A.WRAP_UP}
    # one synthetic ANSWERED + one COMPLETED per call, each observed exactly once (dedup key seq=-1)
    assert await w.one("SELECT count(*) FROM provider_events WHERE seq = -1") == 10
    assert await w.one("SELECT count(*) FROM calls WHERE version > 5") == 0  # no call was moved twice
    await w.assert_invariants()


async def test_the_advisory_lock_makes_a_second_reconciler_stand_down(pool):
    w = await build(pool, agents=1, borrowers=1)
    from smartdialer.dialer.reconciler import LOCK_NAMESPACE
    async with pool.acquire() as holder:
        assert await holder.fetchval("SELECT pg_try_advisory_lock($1, $2)", LOCK_NAMESPACE, w.cid)
        assert (await w.reconcile()).skipped_locked is True  # someone else is already reconciling
        # another campaign is independent:
        assert await holder.fetchval("SELECT pg_try_advisory_lock($1, $2)", LOCK_NAMESPACE, w.cid + 1)
        await holder.execute("SELECT pg_advisory_unlock_all()")
    assert (await w.reconcile()).skipped_locked is False
    assert (await w.reconcile()).skipped_locked is False  # and it released the lock after its own pass


async def test_a_dead_reconcilers_lock_is_released_with_its_connection(pool):
    import asyncpg
    from smartdialer import db
    from smartdialer.dialer.reconciler import LOCK_NAMESPACE
    w = await build(pool, agents=1, borrowers=1)
    dying = await asyncpg.connect(db.test_dsn())
    assert await dying.fetchval("SELECT pg_try_advisory_lock($1, $2)", LOCK_NAMESPACE, w.cid)
    assert (await w.reconcile()).skipped_locked
    await dying.close()  # process died: PostgreSQL drops its session locks, no TTL needed
    await asyncio.sleep(0.1)
    assert not (await w.reconcile()).skipped_locked


async def test_healthy_calls_with_unexpired_leases_are_not_even_asked_about(pool):
    w = await build(pool, agents=3, borrowers=3, answer_rate=1.0)
    fixed_talk(w, 300)
    await w.run(20)
    asked = w.raw.stats.status_calls
    rep = await w.reconcile()
    assert rep.examined == 0 and w.raw.stats.status_calls == asked


async def test_a_slow_but_alive_call_is_re_armed_not_failed(pool):
    cfg = DialerConfig(lease_seconds=10.0)  # shorter than the 30s ring: the lease expires on a healthy call
    w = await build(pool, agents=1, borrowers=1, answer_rate=0.0, cfg=cfg)
    await w.run(14)
    rep = await w.reconcile()
    assert rep.examined == 1 and rep.still_live == 1 and rep.not_found == 0
    (c,) = await w.calls()
    assert c["state"] == "RINGING" and c["lease_until"] > w.sched.now()  # untouched, lease renewed
    assert await w.agent_state(w.agent_ids[0]) is A.DIALING


async def test_zombie_worker_returns_after_its_call_was_reconciled_and_the_call_is_hung_up(pool):
    """A worker that is merely SLOW (place_call takes 90s, lease is 60s) is indistinguishable from a
    dead one. The reconciler fails its call; when the zombie finally gets its answer it finds the
    call terminal and hangs up the call it just created."""
    slow = dataclasses.replace(PROFILE_A, place_error_rate=0.0, place_latency=(90.0, 90.0))
    w = await build(pool, agents=1, borrowers=1, profile=slow, answer_rate=1.0)
    fixed_talk(w, 600)
    await w.worker.tick()
    await w.sched.advance(LEASE)
    rep = await w.reconcile()
    assert rep.not_found == 1  # the provider has not even started the call yet
    first = (await w.calls())[0]["id"]
    assert (await call_row(w, first))["end_reason"] == "WORKER_CRASH"
    w.raw.profile = dataclasses.replace(slow, place_latency=(0.2, 0.2))  # later dials are fast

    await w.run(40)  # t=61..101: zombie's place_call returns at t=90
    assert w.worker.stats.compensating_hangups == 1
    st = await w.sched.run(w.raw.get_call_status(client_ref=first))
    assert (st.state.value, st.reason) == ("FAILED", "CANCELLED")  # hung up
    await w.assert_invariants()
    live = await w.rows("SELECT borrower_id FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')")
    assert len(live) <= 1  # the borrower was never on two live calls


async def test_orphans_left_by_anything_unforeseen_are_repaired(pool):
    w = await build(pool, agents=2, borrowers=2)
    async with pool.acquire() as conn:  # corrupt on purpose: RESERVED agent + RESERVED borrower, no call
        await conn.execute("UPDATE agents SET state='RESERVED', lease_until=$2 WHERE id=$1", w.agent_ids[0], w.sched.now())
        await conn.execute("UPDATE borrowers SET status='RESERVED', attempts=1 WHERE id=$1", w.borrower_ids[0])
    await w.sched.advance(5)
    rep = await w.reconcile()
    assert (rep.orphan_agents, rep.orphan_borrowers) == (1, 1)
    assert await w.agent_state(w.agent_ids[0]) is A.AVAILABLE
    b = await w.borrower(w.borrower_ids[0])
    assert (b["status"], b["attempts"]) == ("PENDING", 0)


# ---- soak: everything going wrong at once --------------------------------------------

async def test_soak_chaotic_provider_agents_vanishing_outage_and_lost_callbacks(pool):
    """Provider B (duplicates, reordering, timeouts) + agents logging off and on at random + a
    provider outage + a window where callbacks are lost + the reconcile loop running throughout.
    Invariants are checked EVERY virtual second; at the end everything must settle."""
    rng = random.Random(42)
    w = await build(pool, agents=15, borrowers=80, provider="B", answer_rate=0.5, talk=25.0, auto_reconcile=True,
                    breaker_kw={"min_samples": 10_000}, seed=7)
    handler = w.worker.events.handle
    offline: dict[int, int] = {}
    for t in range(520):
        if t % 6 == 0:
            a = rng.choice(w.agent_ids)
            if a in offline:
                await w.worker.agents.agent_login(a)
                offline.pop(a)
            else:
                await w.worker.agents.agent_offline(a)
                offline[a] = t
        if t == 150:
            w.raw.start_outage()
        if t == 200:
            w.raw.end_outage()
        if t == 300:
            w.raw.set_event_handler(None)  # the callback endpoint is down...
        if t == 340:
            w.raw.set_event_handler(handler)  # ...and back
        await w.step()
        await w.assert_invariants()

    for a in list(offline):
        await w.worker.agents.agent_login(a)
    await w.quiesce(1500)  # stop dialing; reconciler (every 5s) and late events settle the rest
    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") == 0
    assert {await w.agent_state(a) for a in w.agent_ids} == {A.AVAILABLE}
    inv = await w.assert_invariants()
    assert inv["reserved_borrowers"] == 0 and inv["busy_agents"] == 0
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") == 0
    assert await w.one("SELECT count(*) FROM calls WHERE state = 'COMPLETED'") > 15
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'AGENT_LOST'") > 0  # agents really vanished
