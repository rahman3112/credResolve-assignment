import dataclasses
import random

import pytest

from smartdialer.clock import VirtualClock
from smartdialer.providers.base import (
    CircuitOpenError,
    PlaceCallRequest,
    ProviderRejected,
    ProviderTimeout,
)
from smartdialer.providers.behavior import CallBehavior
from smartdialer.providers.breaker import BreakerState, CircuitBreaker, ProtectedProvider
from smartdialer.providers.mock import PROFILE_A, MockProvider
from smartdialer.scheduler import Scheduler

S = BreakerState


def mk(clock=None, **kw):
    clock = clock or VirtualClock()
    return CircuitBreaker("A", clock, **kw), clock


def call(b, ok: bool):
    b.before_call()
    b.record_success() if ok else b.record_failure()


def trip(b):
    for _ in range(b.min_samples):
        call(b, False)
    assert b.state is S.OPEN


def test_stays_closed_below_min_samples_even_if_all_fail():
    b, _ = mk(min_samples=10)
    for _ in range(9):
        call(b, False)
    assert b.state is S.CLOSED  # too little evidence to condemn a provider


def test_trips_at_failure_ratio_with_enough_samples():
    b, _ = mk(min_samples=10, failure_ratio=0.5)
    for ok in [True] * 5 + [False] * 4:
        call(b, ok)
    assert b.state is S.CLOSED  # 4/9, below min samples anyway
    call(b, False)  # 5/10 = 50%
    assert b.state is S.OPEN


def test_healthy_traffic_with_occasional_failures_does_not_trip():
    b, _ = mk()
    for i in range(200):
        call(b, i % 20 != 0)  # 5% failures, like provider A
    assert b.state is S.CLOSED


def test_old_failures_age_out_of_the_window():
    b, clock = mk(window_seconds=30, min_samples=10)
    for _ in range(9):
        call(b, False)
    clock.advance(31)  # those failures are now history
    for _ in range(9):
        call(b, True)
    call(b, False)
    assert b.state is S.CLOSED
    assert b.health().samples == 10 and b.health().failure_ratio == pytest.approx(0.1)


def test_open_fails_fast_then_probes_after_the_wait():
    b, clock = mk(open_seconds=15)
    trip(b)
    with pytest.raises(CircuitOpenError):
        b.before_call()
    clock.advance(14)
    with pytest.raises(CircuitOpenError):
        b.before_call()
    clock.advance(1)
    b.before_call()  # first probe goes through
    assert b.state is S.HALF_OPEN


def test_half_open_limits_probes_and_closes_after_enough_successes():
    b, clock = mk(open_seconds=15, half_open_probes=2, close_after=2)
    trip(b)
    clock.advance(15)
    b.before_call()
    b.before_call()
    with pytest.raises(CircuitOpenError):
        b.before_call()  # only 2 probes may be in flight
    b.record_success()
    assert b.state is S.HALF_OPEN
    b.record_success()
    assert b.state is S.CLOSED and b.health().samples == 0  # clean slate


def test_failed_probe_reopens_with_doubled_wait_up_to_the_cap():
    b, clock = mk(open_seconds=15, max_open_seconds=40)
    trip(b)
    waits = []
    for _ in range(3):
        clock.advance(100)  # long enough to probe
        b.before_call()
        b.record_failure()
        assert b.state is S.OPEN
        waits.append((b.health().retry_at - clock.now()).total_seconds())
    assert waits == [30, 40, 40]  # 15 -> 30 -> capped at 40


def test_cancelled_probe_frees_its_slot_without_counting():
    b, clock = mk(open_seconds=15, half_open_probes=1)
    trip(b)
    clock.advance(15)
    b.before_call()
    b.abort()
    b.before_call()  # slot is free again; no failure was recorded
    assert b.state is S.HALF_OPEN


def test_transitions_are_logged_for_reports():
    b, clock = mk(open_seconds=15, close_after=1)
    trip(b)
    clock.advance(15)
    call(b, True)
    assert [s for _, s in b.transitions] == [S.OPEN, S.HALF_OPEN, S.CLOSED]


# ---- wrapped around a real mock provider --------------------------------------------

@pytest.fixture
def world():
    sched = Scheduler(VirtualClock())
    profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0)  # only the outage fails calls
    prov = MockProvider("A", profile, CallBehavior(answer_rate=1.0), sched, random.Random(1))
    breaker = CircuitBreaker("A", sched.clock, min_samples=10, open_seconds=15, close_after=2)
    return sched, prov, ProtectedProvider(prov, breaker)


async def burst(sched, protected, first_ref, n):
    async def one(ref):
        try:
            await protected.place_call(PlaceCallRequest(ref, f"+1555{ref:07d}"))
            return "ok"
        except CircuitOpenError:
            return "open"
        except ProviderTimeout:
            return "timeout"

    return await sched.run(sched.gather(*[one(first_ref + i) for i in range(n)]))


async def test_outage_trips_breaker_then_new_calls_fail_fast_without_reaching_provider(world):
    sched, prov, prot = world
    prov.start_outage()
    assert set(await burst(sched, prot, 1, 10)) == {"timeout"}  # slow, expensive failures
    assert prot.breaker.state is S.OPEN

    attempts, t0 = prov.stats.place_attempts, sched.now()
    assert set(await burst(sched, prot, 100, 50)) == {"open"}
    assert prov.stats.place_attempts == attempts  # provider untouched: no more load on a dying system
    assert sched.now() == t0  # and instant: agents are not held hostage for 5s timeouts


async def test_cleanup_calls_still_reach_the_provider_while_the_breaker_is_open(world):
    sched, prov, prot = world
    placed = await sched.run(prot.place_call(PlaceCallRequest(1, "+15550000001")))
    prov.start_outage()
    await burst(sched, prot, 10, 10)
    assert prot.breaker.state is S.OPEN
    hangups_before = prov.stats.status_calls
    with pytest.raises(ProviderTimeout):  # reached the provider (and timed out); not CircuitOpenError
        await sched.run(prot.get_call_status(provider_call_id=placed.provider_call_id))
    assert prov.stats.status_calls == hangups_before + 1


async def test_recovery_probes_close_the_breaker(world):
    sched, prov, prot = world
    prov.start_outage()
    await burst(sched, prot, 1, 10)
    assert prot.breaker.state is S.OPEN
    prov.end_outage()

    await sched.advance(15)
    results = await burst(sched, prot, 200, 3)  # probes
    assert results.count("ok") >= 2 and prot.breaker.state is S.CLOSED
    assert set(await burst(sched, prot, 300, 20)) == {"ok"}
    assert [s for _, s in prot.breaker.transitions] == [S.OPEN, S.HALF_OPEN, S.CLOSED]


async def test_rejected_numbers_are_not_provider_failures(world):
    sched, prov, prot = world

    async def bad(ref):
        with pytest.raises(ProviderRejected):
            await prot.place_call(PlaceCallRequest(ref, "+0000"))

    await sched.run(sched.gather(*[bad(i) for i in range(1, 40)]))
    assert prot.breaker.state is S.CLOSED
    assert prot.health().healthy


async def test_health_snapshot_reports_ratio_and_state(world):
    sched, prov, prot = world
    prov.start_outage()
    await burst(sched, prot, 1, 10)
    h = prot.health()
    assert (h.state, h.failure_ratio, h.samples, h.healthy) == (S.OPEN, 1.0, 10, False)
    assert h.retry_at is not None
