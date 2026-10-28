"""Deterministic virtual time for asyncio (no real sleeping).

Mock providers have latencies, timeouts and delayed events; the simulator runs hours of
dialing in seconds. Both need "wait 3 seconds" to mean *virtual* seconds.

How it works: tasks are started with `spawn()`; `sleep()` parks a task on a timer instead of
the real clock. The scheduler counts how many of its tasks are currently runnable. When the
count hits zero everything is waiting on a timer, so it jumps the VirtualClock to the earliest
timer and wakes it. Order is (time, insertion order), so a run is reproducible.

Rules (violations raise rather than hang):
  * `sleep()` only works inside a task started by `spawn()`/`run()`/`gather()`.
  * Use `Scheduler.gather` rather than asyncio.gather inside such tasks: asyncio.gather
    creates tasks the scheduler cannot count.
Real I/O (the database) inside a spawned task is fine: the task simply stays "runnable"
until it finishes or sleeps, and `settle()` waits for it.
"""

from __future__ import annotations

import asyncio
import contextvars
import heapq
import itertools
import time
from collections.abc import Awaitable, Callable, Coroutine
from datetime import datetime, timedelta
from typing import Any

from smartdialer.clock import VirtualClock

_in_scheduler_task: contextvars.ContextVar[bool] = contextvars.ContextVar("in_scheduler_task", default=False)


class Timer:
    __slots__ = ("when", "seq", "fut", "fn", "cancelled", "fired")

    def __init__(self, when: datetime, seq: int, fut=None, fn=None):
        self.when, self.seq, self.fut, self.fn = when, seq, fut, fn
        self.cancelled = self.fired = False

    def __lt__(self, other: Timer) -> bool:
        return (self.when, self.seq) < (other.when, other.seq)

    def cancel(self) -> None:
        self.cancelled = True


class _Group:
    """Children of one gather(): lets the last one to finish wake the parent atomically."""
    __slots__ = ("remaining", "fut", "abandoned")

    def __init__(self, n: int, fut):
        self.remaining, self.fut, self.abandoned = n, fut, False


class Scheduler:
    SETTLE_REAL_TIMEOUT = 60.0  # real seconds; a tracked task stuck on non-virtual I/O

    def __init__(self, clock: VirtualClock):
        self.clock = clock
        self._heap: list[Timer] = []
        self._seq = itertools.count()
        self._active = 0
        self.background_errors: list[BaseException] = []

    def now(self) -> datetime:
        return self.clock.now()

    # ---- tasks -----------------------------------------------------------------------
    def spawn(self, coro: Coroutine[Any, Any, Any], name: str | None = None) -> asyncio.Task:
        return self._spawn(coro, None, name)

    def _spawn(self, coro, group: _Group | None, name: str | None = None) -> asyncio.Task:
        self._active += 1
        return asyncio.get_running_loop().create_task(self._run(coro, group), name=name)

    async def _run(self, coro, group: _Group | None):
        _in_scheduler_task.set(True)
        try:
            return await coro
        except asyncio.CancelledError:
            raise
        except BaseException as e:  # keep failures of fire-and-forget tasks visible to tests
            self.background_errors.append(e)
            raise
        finally:
            if group is not None:
                group.remaining -= 1
                if group.remaining == 0 and not group.abandoned:
                    # Hand the "runnable" token to the waiting parent BEFORE giving up our
                    # own, so the count never dips to zero while work is still pending.
                    self._active += 1
                    group.fut.set_result(None)
            self._active -= 1

    async def gather(self, *coros: Coroutine) -> list:
        """Run coroutines concurrently as tracked tasks and wait for all. Only callable from
        inside a tracked task. While waiting, the caller is parked (not runnable), exactly
        like a sleeper; otherwise the scheduler would think it is still busy and never
        advance time."""
        if not coros:
            return []
        group = _Group(len(coros), asyncio.get_running_loop().create_future())
        tasks = [self._spawn(c, group) for c in coros]
        self._active -= 1  # parked until the LAST child finishes (see _run: it re-activates us)
        try:
            await group.fut
        except asyncio.CancelledError:
            group.abandoned = True
            if not group.fut.done():
                self._active += 1  # runnable again, only to unwind
            raise
        errors = [t.exception() for t in tasks]  # retrieves every exception: no "never retrieved" noise
        if any(errors):
            raise next(e for e in errors if e)
        return [t.result() for t in tasks]

    # ---- timers ----------------------------------------------------------------------
    async def sleep(self, delay: float) -> None:
        if not _in_scheduler_task.get():
            raise RuntimeError("Scheduler.sleep() outside a spawned task: virtual time would never advance")
        if delay <= 0:
            await asyncio.sleep(0)
            return
        fut = asyncio.get_running_loop().create_future()
        timer = Timer(self.now() + timedelta(seconds=delay), next(self._seq), fut=fut)
        heapq.heappush(self._heap, timer)
        self._active -= 1  # parked: not runnable until the timer fires
        try:
            await fut
        except asyncio.CancelledError:
            timer.cancel()
            if not timer.fired:
                self._active += 1  # it is runnable again, just to unwind
            raise

    def call_later(self, delay: float, fn: Callable[[], Awaitable]) -> Timer:
        """Run `fn()` as a spawned task `delay` virtual seconds from now."""
        timer = Timer(self.now() + timedelta(seconds=max(0.0, delay)), next(self._seq), fn=fn)
        heapq.heappush(self._heap, timer)
        return timer

    def _peek(self) -> Timer | None:
        while self._heap and self._heap[0].cancelled:
            heapq.heappop(self._heap)
        return self._heap[0] if self._heap else None

    def _fire(self, timer: Timer) -> None:
        self.clock.advance_to(timer.when)
        timer.fired = True
        if timer.fut is not None:
            if not timer.fut.done():
                self._active += 1
                timer.fut.set_result(None)
        else:
            self.spawn(timer.fn())

    # ---- driving ---------------------------------------------------------------------
    async def settle(self) -> None:
        """Let every runnable task run until all are finished or parked on a timer."""
        spins, started = 0, time.monotonic()
        while self._active > 0:
            await asyncio.sleep(0 if spins < 200 else 0.001)  # past 200 spins: waiting on real I/O
            spins += 1
            if time.monotonic() - started > self.SETTLE_REAL_TIMEOUT:
                raise RuntimeError("settle() timed out: a scheduler task is blocked on something non-virtual")

    async def advance_to(self, target: datetime) -> None:
        """Run everything due up to `target`, then leave the clock exactly at `target`."""
        while True:
            await self.settle()
            timer = self._peek()
            if timer is None or timer.when > target:
                break
            heapq.heappop(self._heap)
            self._fire(timer)
        self.clock.advance_to(target)
        await self.settle()

    async def advance(self, seconds: float) -> None:
        await self.advance_to(self.now() + timedelta(seconds=seconds))

    async def run(self, coro: Coroutine, max_virtual_seconds: float = 24 * 3600):
        """Run `coro` to completion, advancing virtual time as needed; return its result."""
        task = self.spawn(coro)
        limit = self.now() + timedelta(seconds=max_virtual_seconds)
        while True:
            await self.settle()
            if task.done():
                return task.result()
            timer = self._peek()
            if timer is None:
                raise RuntimeError("deadlock: task is waiting and no timer is pending")
            if timer.when > limit:
                raise RuntimeError("virtual time limit exceeded")
            heapq.heappop(self._heap)
            self._fire(timer)

    @property
    def pending_timers(self) -> int:
        return sum(1 for t in self._heap if not t.cancelled)
