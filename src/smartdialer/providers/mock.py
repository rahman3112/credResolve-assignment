"""Mock telecom providers. Same interface, deliberately different personalities.

ProviderA: fast, ~2% transient failures, events arrive promptly and in order, no duplicates.
ProviderB: slow, ~5% timeouts (half of them AMBIGUOUS: the call was placed anyway), events
           duplicated and delivered with enough jitter to arrive out of order.
Both: start_outage() / end_outage() simulate the provider going dark.

Everything runs on the virtual Scheduler and a seeded RNG, so a run is reproducible.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from smartdialer.providers.base import (
    EventHandler,
    PlaceCallRequest,
    PlacedCall,
    ProviderCallState,
    ProviderCallStatus,
    ProviderError,
    ProviderRejected,
    ProviderTimeout,
)
from smartdialer.providers.behavior import CallBehavior, Outcome
from smartdialer.repo.events import ProviderEvent, ProviderEventType
from smartdialer.scheduler import Scheduler

_TERMINAL = {ProviderCallState.COMPLETED, ProviderCallState.FAILED}


@dataclass(frozen=True)
class ProviderProfile:
    place_latency: tuple[float, float]  # uniform seconds
    status_latency: tuple[float, float]
    place_error_rate: float  # fast failure (HTTP 503): call NOT placed
    place_timeout_rate: float
    timeout_seconds: float
    timeout_but_placed_rate: float  # of timeouts, how many actually placed the call
    event_delay: tuple[float, float]  # uniform delivery delay per event copy
    duplicate_rate: float  # chance an event is sent again
    max_extra_copies: int


PROFILE_A = ProviderProfile(
    place_latency=(0.1, 0.4), status_latency=(0.05, 0.2),
    place_error_rate=0.02, place_timeout_rate=0.0, timeout_seconds=5.0, timeout_but_placed_rate=0.0,
    # Constant delay + FIFO timers => strictly ordered delivery, no jitter.
    event_delay=(0.1, 0.1), duplicate_rate=0.0, max_extra_copies=0)

PROFILE_B = ProviderProfile(
    place_latency=(1.0, 3.0), status_latency=(0.5, 2.0),
    place_error_rate=0.02, place_timeout_rate=0.05, timeout_seconds=10.0, timeout_but_placed_rate=0.5,
    # Jitter (0.2-15s) is comparable to the gaps between events (ring time 6-24s, talk time
    # often a few seconds), so reordering is common; duplicates get independent delays.
    event_delay=(0.2, 15.0), duplicate_rate=0.20, max_extra_copies=2)


@dataclass
class _Call:
    provider_call_id: str
    client_ref: int
    phone: str
    state: ProviderCallState = ProviderCallState.INITIATED
    reason: str | None = None
    was_answered: bool = False
    seq: int = 0  # per-call event sequence; duplicates reuse the same seq


@dataclass
class ProviderStats:
    place_attempts: int = 0
    placed: int = 0  # distinct calls that actually exist at the provider
    errors: int = 0
    timeouts: int = 0
    timeouts_but_placed: int = 0
    events_emitted: int = 0
    event_copies_sent: int = 0
    events_dropped_in_outage: int = 0
    hangups: int = 0
    hangups_on_answered: int = 0  # we hung up on a borrower who had ALREADY answered (ground truth: dead air)
    status_calls: int = 0


class MockProvider:
    def __init__(self, name: str, profile: ProviderProfile, behavior: CallBehavior,
                 sched: Scheduler, rng: random.Random):
        self.name = name
        self.profile = profile
        self.behavior = behavior
        self.sched = sched
        # Every random draw is keyed by WHAT it is about (call id + step), never taken from one shared
        # stream. A shared stream makes results depend on which concurrent task happened to run first,
        # and with real database I/O in the loop that order varies run to run. Keyed draws make a
        # seeded run reproducible whatever order tasks finish in.
        self._seed = rng.getrandbits(64)
        self._counters: dict[tuple, int] = {}
        self.stats = ProviderStats()
        self._calls: dict[str, _Call] = {}
        self._by_ref: dict[int, _Call] = {}
        self._handler: EventHandler | None = None
        self._outage = False

    def _r(self, *key) -> random.Random:
        """A fresh RNG for one purpose, e.g. ("place", call_id, attempt). Deterministic across runs
        and processes (string seeding is hashed with SHA-512, not Python's randomised hash())."""
        return random.Random(f"{self._seed}|" + "|".join(map(str, key)))

    def _nth(self, *key) -> int:
        """How many times this key has been used (separates retries of the same call)."""
        n = self._counters[key] = self._counters.get(key, 0) + 1
        return n

    # ---- control surface for tests / simulator ---------------------------------------
    def start_outage(self) -> None:
        """Provider goes dark: every API call times out, event callbacks are lost. Calls
        already in progress keep progressing *inside* the provider, so after end_outage()
        get_call_status tells the truth about what happened meanwhile."""
        self._outage = True

    def end_outage(self) -> None:
        self._outage = False

    @property
    def in_outage(self) -> bool:
        return self._outage

    def set_event_handler(self, handler: EventHandler) -> None:
        self._handler = handler

    # ---- Provider interface ----------------------------------------------------------
    async def place_call(self, req: PlaceCallRequest) -> PlacedCall:
        p = self.profile
        rng = self._r("place", req.client_ref, self._nth("place", req.client_ref))
        self.stats.place_attempts += 1
        await self.sched.sleep(rng.uniform(*p.place_latency))

        if self._outage:
            return await self._timeout(placed=False)
        if req.phone.startswith("+0"):  # test hook: a permanently invalid number
            raise ProviderRejected(f"invalid number {req.phone}")

        # Idempotency: a retry of an earlier (possibly timed-out) request finds its call.
        existing = self._by_ref.get(req.client_ref)
        if existing is not None:
            return PlacedCall(existing.provider_call_id)

        roll = rng.random()
        if roll < p.place_error_rate:
            self.stats.errors += 1
            raise ProviderError("503 service unavailable")
        if roll < p.place_error_rate + p.place_timeout_rate:
            placed = rng.random() < p.timeout_but_placed_rate
            if placed:
                self._start_call(req)
                self.stats.timeouts_but_placed += 1
            return await self._timeout(placed=placed)

        return PlacedCall(self._start_call(req).provider_call_id)

    async def hangup(self, provider_call_id: str) -> None:
        await self.sched.sleep(self._r("hangup", provider_call_id, self._nth("hangup", provider_call_id))
                               .uniform(*self.profile.status_latency))
        if self._outage:
            return await self._timeout(placed=False)
        self.stats.hangups += 1
        call = self._calls.get(provider_call_id)
        if call is None or call.state in _TERMINAL:
            return  # idempotent
        if call.state is ProviderCallState.ANSWERED:
            self.stats.hangups_on_answered += 1
            self._finish(call, ProviderCallState.COMPLETED, None)
        else:
            self._finish(call, ProviderCallState.FAILED, "CANCELLED")

    async def get_call_status(self, *, provider_call_id: str | None = None,
                              client_ref: int | None = None) -> ProviderCallStatus:
        if (provider_call_id is None) == (client_ref is None):
            raise ValueError("pass exactly one of provider_call_id / client_ref")
        self.stats.status_calls += 1
        key = ("status", provider_call_id or f"ref{client_ref}")
        await self.sched.sleep(self._r(*key, self._nth(*key)).uniform(*self.profile.status_latency))
        if self._outage:
            return await self._timeout(placed=False)
        call = self._calls.get(provider_call_id) if provider_call_id else self._by_ref.get(client_ref)
        if call is None:
            return ProviderCallStatus(ProviderCallState.NOT_FOUND, provider_call_id, client_ref)
        return ProviderCallStatus(call.state, call.provider_call_id, call.client_ref,
                                  call.reason, call.was_answered)

    # ---- internals -------------------------------------------------------------------
    async def _timeout(self, *, placed: bool):
        self.stats.timeouts += 1
        await self.sched.sleep(self.profile.timeout_seconds)
        raise ProviderTimeout(f"{self.name} timed out after {self.profile.timeout_seconds}s")

    def _start_call(self, req: PlaceCallRequest) -> _Call:
        # The provider's id is derived from our call id: an arrival-order counter would hand the same
        # call a different id from run to run.
        call = _Call(f"{self.name}-{req.client_ref}", req.client_ref, req.phone)
        self._calls[call.provider_call_id] = call
        self._by_ref[req.client_ref] = call
        self.stats.placed += 1
        rng = self._r("script", req.client_ref)
        script = self.behavior.draw(rng)  # the borrower's fate is decided now, by the seeded RNG

        if script.outcome is Outcome.BUSY:
            self.sched.call_later(1.0, lambda: self._advance(call, ProviderCallState.FAILED, "BUSY"))
            return call
        ring_start = rng.uniform(0.3, 1.5)
        self.sched.call_later(ring_start, lambda: self._advance(call, ProviderCallState.RINGING, None))
        if script.outcome is Outcome.ANSWER:
            t_answer = ring_start + script.ring_seconds
            self.sched.call_later(t_answer, lambda: self._advance(call, ProviderCallState.ANSWERED, None))
            self.sched.call_later(t_answer + script.talk_seconds,
                                  lambda: self._advance(call, ProviderCallState.COMPLETED, None))
        else:
            self.sched.call_later(ring_start + script.ring_seconds,
                                  lambda: self._advance(call, ProviderCallState.FAILED, "NO_ANSWER"))
        return call

    async def _advance(self, call: _Call, state: ProviderCallState, reason: str | None) -> None:
        if call.state in _TERMINAL:  # e.g. we hung up before the borrower answered
            return
        self._finish(call, state, reason)

    def _finish(self, call: _Call, state: ProviderCallState, reason: str | None) -> None:
        call.state, call.reason = state, reason
        if state is ProviderCallState.ANSWERED:
            call.was_answered = True
        call.seq += 1
        etype = {
            ProviderCallState.RINGING: ProviderEventType.RINGING,
            ProviderCallState.ANSWERED: ProviderEventType.ANSWERED,
            ProviderCallState.COMPLETED: ProviderEventType.COMPLETED,
            ProviderCallState.FAILED: ProviderEventType.FAILED,
        }[state]
        self._emit(ProviderEvent(self.name, call.provider_call_id, etype, seq=call.seq,
                                 client_ref=call.client_ref, occurred_at=self.sched.now(), reason=reason))

    def _emit(self, ev: ProviderEvent) -> None:
        self.stats.events_emitted += 1
        rng = self._r("emit", ev.provider_call_id, ev.seq)
        copies = 1
        while copies <= self.profile.max_extra_copies and rng.random() < self.profile.duplicate_rate:
            copies += 1
        for _ in range(copies):
            self.stats.event_copies_sent += 1
            self.sched.call_later(rng.uniform(*self.profile.event_delay), lambda e=ev: self._deliver(e))

    async def _deliver(self, ev: ProviderEvent) -> None:
        if self._outage:
            self.stats.events_dropped_in_outage += 1  # callback endpoint unreachable: lost for good
            return
        if self._handler is not None:
            await self._handler(ev)


def make_provider_a(sched: Scheduler, behavior: CallBehavior, seed: int = 1) -> MockProvider:
    return MockProvider("A", PROFILE_A, behavior, sched, random.Random(seed))


def make_provider_b(sched: Scheduler, behavior: CallBehavior, seed: int = 2) -> MockProvider:
    return MockProvider("B", PROFILE_B, behavior, sched, random.Random(seed))
