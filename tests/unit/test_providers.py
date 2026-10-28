"""Mock providers: each personality, and the failure modes the dialer must survive."""

import dataclasses
import random
from collections import defaultdict
from datetime import timedelta

import pytest

from smartdialer.clock import VirtualClock
from smartdialer.providers.base import (
    PlaceCallRequest,
    ProviderCallState,
    ProviderError,
    ProviderRejected,
    ProviderTimeout,
)
from smartdialer.providers.behavior import CallBehavior
from smartdialer.providers.mock import PROFILE_A, MockProvider, make_provider_a, make_provider_b
from smartdialer.repo.events import ProviderEventType as T
from smartdialer.scheduler import Scheduler
from smartdialer.states import CallDecision, CallState, decide_call_transition

ANSWER_ALWAYS = dict(answer_rate=1.0, mean_talk_seconds=30.0)


@pytest.fixture
def sched():
    return Scheduler(VirtualClock())


def collect(provider):
    got = []

    async def handler(ev):
        got.append(ev)

    provider.set_event_handler(handler)
    return got


async def place_many(sched, provider, n):
    """Place n calls concurrently; returns (placed, errors)."""
    async def one(ref):
        try:
            return await provider.place_call(PlaceCallRequest(ref, f"+1555{ref:07d}"))
        except ProviderError as e:
            return e

    results = await sched.run(sched.gather(*[one(i) for i in range(1, n + 1)]))
    return results


def by_call(events):
    d = defaultdict(list)
    for e in events:
        d[e.provider_call_id].append(e)
    return d


# ---- Provider A ---------------------------------------------------------------------

async def test_provider_a_is_fast_ordered_and_duplicate_free(sched):
    a = make_provider_a(sched, CallBehavior(**ANSWER_ALWAYS))
    events = collect(a)
    results = await place_many(sched, a, 600)
    await sched.advance(900)

    errors = [r for r in results if isinstance(r, Exception)]
    assert 0 < len(errors) < 30  # ~2% transient failures, never a timeout
    assert not any(isinstance(e, ProviderTimeout) for e in errors)
    assert a.stats.event_copies_sent == a.stats.events_emitted  # no duplicates, ever
    for evs in by_call(events).values():
        assert [e.type for e in evs] == [T.RINGING, T.ANSWERED, T.COMPLETED]
        assert [e.seq for e in evs] == [1, 2, 3]  # delivered in order
        assert all(e.client_ref is not None for e in evs)


async def test_provider_a_place_latency_is_sub_second(sched):
    a = make_provider_a(sched, CallBehavior(**ANSWER_ALWAYS), seed=7)
    t0 = sched.now()
    await sched.run(a.place_call(PlaceCallRequest(1, "+15550000001")))
    assert (sched.now() - t0).total_seconds() < 0.5


# ---- Provider B ---------------------------------------------------------------------

async def test_provider_b_is_slow_duplicates_and_reorders(sched):
    b = make_provider_b(sched, CallBehavior(**ANSWER_ALWAYS))
    events = collect(b)
    results = await place_many(sched, b, 300)
    await sched.advance(1200)

    errs = [r for r in results if isinstance(r, Exception)]
    assert any(isinstance(e, ProviderTimeout) for e in errs)  # occasional timeouts
    assert b.stats.event_copies_sent > b.stats.events_emitted  # duplicates were sent
    reordered = 0
    for evs in by_call(events).values():
        seqs = [e.seq for e in evs]
        reordered += seqs != sorted(seqs)
    assert reordered > 20  # plenty of calls saw events out of order


async def test_replaying_provider_b_chaos_through_the_state_machine_converges(sched):
    """Cross-check with Phase 1: whatever B does to the stream, folding it through
    decide_call_transition lands every call in the state the provider actually ended in."""
    b = make_provider_b(sched, CallBehavior(answer_rate=0.5, mean_talk_seconds=20.0), seed=11)
    events = collect(b)
    await place_many(sched, b, 300)
    await sched.advance(3600)

    to_state = {T.RINGING: CallState.RINGING, T.ANSWERED: CallState.ANSWERED,
                T.COMPLETED: CallState.COMPLETED, T.FAILED: CallState.FAILED}
    checked = 0
    for pcid, evs in by_call(events).items():
        state, applied = CallState.INITIATED, 0
        for e in evs:  # arrival order, duplicates included
            if decide_call_transition(state, to_state[e.type]) is CallDecision.APPLY:
                state, applied = to_state[e.type], applied + 1
        truth = (await sched.run(b.get_call_status(provider_call_id=pcid))).state
        assert state.value == truth.value, (pcid, [e.type for e in evs])
        assert applied <= 3  # never more transitions than distinct ranks
        checked += 1
    assert checked > 250


async def test_timeout_is_ambiguous_and_place_call_is_idempotent_on_client_ref(sched):
    b = make_provider_b(sched, CallBehavior(**ANSWER_ALWAYS))
    results = await place_many(sched, b, 300)
    timed_out = [i + 1 for i, r in enumerate(results) if isinstance(r, ProviderTimeout)]
    assert timed_out
    seen_placed = seen_not_placed = 0
    for ref in timed_out:
        st = await sched.run(b.get_call_status(client_ref=ref))
        if st.state is ProviderCallState.NOT_FOUND:
            seen_not_placed += 1
        else:
            seen_placed += 1
            placed_before = b.stats.placed
            again = await sched.run(b.place_call(PlaceCallRequest(ref, "+x")))
            assert again.provider_call_id == st.provider_call_id  # same call, not a second ring
            assert b.stats.placed == placed_before
    # Both cases happen: that is precisely why the dialer must look before it retries.
    assert seen_placed and seen_not_placed


# ---- outage -------------------------------------------------------------------------

async def test_outage_times_out_new_calls_drops_events_and_keeps_truth_inside(sched):
    a = make_provider_a(sched, CallBehavior(answer_rate=1.0, mean_talk_seconds=60.0))
    a.profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0)
    events = collect(a)
    placed = await sched.run(a.place_call(PlaceCallRequest(1, "+15550000001")))
    await sched.advance(5)  # RINGING delivered
    delivered_before = len(events)
    assert delivered_before >= 1

    a.start_outage()
    t0 = sched.now()
    with pytest.raises(ProviderTimeout):
        await sched.run(a.place_call(PlaceCallRequest(2, "+15550000002")))
    assert (sched.now() - t0).total_seconds() >= a.profile.timeout_seconds  # a slow failure, not a fast one
    with pytest.raises(ProviderTimeout):
        await sched.run(a.get_call_status(provider_call_id=placed.provider_call_id))

    await sched.advance(300)  # the existing call answers and completes during the outage...
    assert len(events) == delivered_before  # ...but no callbacks got through
    assert a.stats.events_dropped_in_outage >= 2

    a.end_outage()
    st = await sched.run(a.get_call_status(provider_call_id=placed.provider_call_id))
    assert st.state is ProviderCallState.COMPLETED and st.was_answered  # reconcilable afterwards


# ---- hangup, validation, determinism ------------------------------------------------

async def test_hangup_before_answer_fails_with_cancelled_and_is_idempotent(sched):
    a = make_provider_a(sched, CallBehavior(**ANSWER_ALWAYS))
    a.profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0)
    events = collect(a)
    placed = await sched.run(a.place_call(PlaceCallRequest(1, "+15550000001")))
    await sched.run(a.hangup(placed.provider_call_id))
    await sched.run(a.hangup(placed.provider_call_id))
    await sched.advance(300)  # the scripted ANSWERED/COMPLETED must not happen
    st = await sched.run(a.get_call_status(provider_call_id=placed.provider_call_id))
    assert (st.state, st.reason, st.was_answered) == (ProviderCallState.FAILED, "CANCELLED", False)
    assert [e.type for e in events][-1] is T.FAILED and T.ANSWERED not in [e.type for e in events]


async def test_hangup_after_answer_completes_the_call(sched):
    a = make_provider_a(sched, CallBehavior(answer_rate=1.0, mean_talk_seconds=600.0))
    a.profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0)
    placed = await sched.run(a.place_call(PlaceCallRequest(1, "+15550000001")))
    await sched.advance(40)
    await sched.run(a.hangup(placed.provider_call_id))
    st = await sched.run(a.get_call_status(provider_call_id=placed.provider_call_id))
    assert st.state is ProviderCallState.COMPLETED and st.was_answered


async def test_invalid_number_is_rejected_not_a_health_failure(sched):
    a = make_provider_a(sched, CallBehavior())
    with pytest.raises(ProviderRejected):
        await sched.run(a.place_call(PlaceCallRequest(1, "+0000")))
    assert not isinstance(ProviderRejected("x"), ProviderError)  # the breaker must not count it


async def test_status_of_unknown_call_and_argument_validation(sched):
    a = make_provider_a(sched, CallBehavior())
    st = await sched.run(a.get_call_status(client_ref=99))
    assert st.state is ProviderCallState.NOT_FOUND
    with pytest.raises(ValueError):
        await sched.run(a.get_call_status())
    with pytest.raises(ValueError):
        await sched.run(a.get_call_status(provider_call_id="x", client_ref=1))


async def test_same_seed_same_trace():
    async def trace():
        s = Scheduler(VirtualClock())
        b = make_provider_b(s, CallBehavior(answer_rate=0.5), seed=5)
        got = collect(b)
        await place_many(s, b, 60)
        await s.advance(600)
        return [(e.provider_call_id, e.type, e.seq) for e in got]

    assert await trace() == await trace()


def test_behavior_answer_rate_and_talk_time_follow_the_knobs():
    rng = random.Random(3)
    beh = CallBehavior(answer_rate=0.2, mean_talk_seconds=120.0)
    draws = [beh.draw(rng) for _ in range(5000)]
    answered = [d for d in draws if d.talk_seconds > 0]
    assert 0.18 < len(answered) / len(draws) < 0.22
    assert 105 < sum(d.talk_seconds for d in answered) / len(answered) < 135
    beh.answer_rate = 0.7  # scenario D mutates the world mid-run
    assert sum(beh.draw(rng).talk_seconds > 0 for _ in range(2000)) / 2000 > 0.65

