import asyncio
from datetime import timedelta

import pytest

from smartdialer.clock import VirtualClock
from smartdialer.scheduler import Scheduler


@pytest.fixture
def sched():
    return Scheduler(VirtualClock())


async def test_sleepers_wake_in_time_order_and_clock_jumps(sched):
    start, order = sched.now(), []

    async def sleeper(name, d):
        await sched.sleep(d)
        order.append((name, (sched.now() - start).total_seconds()))

    await sched.run(sched.gather(sleeper("c", 3), sleeper("a", 1), sleeper("b", 2)))
    assert order == [("a", 1.0), ("b", 2.0), ("c", 3.0)]


async def test_a_day_of_virtual_time_costs_no_real_time(sched):
    async def main():
        for _ in range(24):
            await sched.sleep(3600)
        return "done"

    start = sched.now()
    assert await sched.run(main()) == "done"
    assert sched.now() - start == timedelta(hours=24)


async def test_equal_times_run_in_insertion_order(sched):
    seen = []

    async def mark(x):
        seen.append(x)

    for x in "abcde":
        sched.call_later(1.0, lambda x=x: mark(x))
    await sched.advance(2)
    assert seen == list("abcde")


async def test_advance_runs_only_what_is_due_and_lands_exactly_on_target(sched):
    seen = []

    async def mark(x):
        seen.append(x)

    sched.call_later(5, lambda: mark("early"))
    sched.call_later(15, lambda: mark("late"))
    start = sched.now()
    await sched.advance(10)
    assert seen == ["early"] and sched.now() == start + timedelta(seconds=10)
    await sched.advance(10)
    assert seen == ["early", "late"]


async def test_cancelled_timer_never_fires(sched):
    seen = []

    async def mark():
        seen.append(1)

    sched.call_later(1, mark).cancel()
    await sched.advance(5)
    assert seen == []


async def test_cancelling_a_sleeping_task_keeps_accounting_correct(sched):
    async def sleeper():
        await sched.sleep(100)

    task = sched.spawn(sleeper())
    await sched.settle()  # parked on its timer
    task.cancel()
    await asyncio.sleep(0)  # let the task process its cancellation
    await sched.settle()  # must return: the cancelled task is accounted for
    assert task.cancelled()
    await sched.advance(200)  # and later timers still work
    assert sched._active == 0


async def test_sleep_outside_a_spawned_task_fails_loudly(sched):
    with pytest.raises(RuntimeError, match="outside a spawned task"):
        await sched.sleep(1)


async def test_run_detects_deadlock(sched):
    async def stuck():
        await asyncio.get_running_loop().create_future()  # nothing will ever resolve this

    with pytest.raises(RuntimeError):
        # settle() would spin on a non-virtual wait; with no tracked sleeper it times out fast
        sched.SETTLE_REAL_TIMEOUT = 0.3
        await sched.run(stuck())


async def test_exceptions_propagate_from_run_and_are_recorded_for_background_tasks(sched):
    async def boom():
        raise ValueError("x")

    with pytest.raises(ValueError):
        await sched.run(boom())
    sched.call_later(1, boom)
    await sched.advance(2)
    assert len(sched.background_errors) == 2


async def test_two_runs_are_identical(sched):
    async def trace():
        s = Scheduler(VirtualClock())
        out = []

        async def w(i):
            await s.sleep((i * 7) % 5 + 0.5)
            out.append((i, s.now().timestamp()))

        await s.run(s.gather(*[w(i) for i in range(20)]))
        return out

    assert await trace() == await trace()
