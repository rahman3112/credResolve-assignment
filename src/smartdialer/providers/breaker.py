"""Circuit breaker around a provider, and the health signal the Safety Controller reads.

CLOSED     normal. Every place_call result goes into a rolling time window. When the window has
           at least `min_samples` results and >= `failure_ratio` of them failed -> OPEN.
OPEN       place_call fails FAST with CircuitOpenError; the provider is not contacted. After
           the wait the next place_call is let through as a probe -> HALF_OPEN.
HALF_OPEN  at most `half_open_probes` probe calls in flight. `close_after` successes ->
           CLOSED (window cleared). Any failure -> OPEN again, with the wait doubled (up to
           `max_open_seconds`) so a flapping provider is not hammered.

What the breaker does NOT do: it never stops progressive dialing outright. OPEN just means
"don't send new load"; the Safety Controller reads `health()` and refuses predictive
over-dialing, and the probes are how we find out the provider is back. If the controller
blocked *all* dialing while OPEN, nothing would ever probe and the breaker could never close.

Only place_call is gated and counted. hangup / get_call_status are the tools for cleaning up
after an outage (reconciler, releasing calls), so they must keep working while the breaker is
open (D-16).
"""

from __future__ import annotations

import enum
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from smartdialer.clock import Clock
from smartdialer.providers.base import (
    CircuitOpenError,
    EventHandler,
    PlaceCallRequest,
    PlacedCall,
    Provider,
    ProviderCallStatus,
    ProviderRejected,
)


class BreakerState(enum.StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    provider: str
    state: BreakerState
    failure_ratio: float  # over the current window; 0.0 when there are no samples
    samples: int
    retry_at: datetime | None  # when an OPEN breaker will next allow a probe

    @property
    def healthy(self) -> bool:
        return self.state is BreakerState.CLOSED


class CircuitBreaker:
    def __init__(self, name: str, clock: Clock, *, window_seconds: float = 30.0, min_samples: int = 10,
                 failure_ratio: float = 0.5, open_seconds: float = 15.0, max_open_seconds: float = 120.0,
                 half_open_probes: int = 3, close_after: int = 2):
        self.name, self.clock = name, clock
        self.window = timedelta(seconds=window_seconds)
        self.min_samples, self.threshold = min_samples, failure_ratio
        self.base_open, self.max_open = open_seconds, max_open_seconds
        self.half_open_probes, self.close_after = half_open_probes, close_after
        self._results: deque[tuple[datetime, bool]] = deque()
        self._state = BreakerState.CLOSED
        self._open_for = open_seconds
        self._retry_at: datetime | None = None
        self._probes_in_flight = 0
        self._probe_successes = 0
        self.transitions: list[tuple[datetime, BreakerState]] = []  # for reports and tests

    # ---- gate ------------------------------------------------------------------------
    def before_call(self) -> None:
        """Raise CircuitOpenError if the call may not proceed; otherwise reserve a probe slot
        when half-open. Must be paired with record_success / record_failure / abort."""
        now = self.clock.now()
        if self._state is BreakerState.OPEN:
            if now < self._retry_at:
                raise CircuitOpenError(f"{self.name} circuit open until {self._retry_at.isoformat()}")
            self._set(BreakerState.HALF_OPEN)
            self._probes_in_flight, self._probe_successes = 0, 0
        if self._state is BreakerState.HALF_OPEN:
            if self._probes_in_flight >= self.half_open_probes:
                raise CircuitOpenError(f"{self.name} half-open: probe slots busy")
            self._probes_in_flight += 1

    def record_success(self) -> None:
        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._probe_successes += 1
            if self._probe_successes >= self.close_after:
                self._results.clear()
                self._open_for = self.base_open
                self._set(BreakerState.CLOSED)
            return
        self._add(True)

    def record_failure(self) -> None:
        now = self.clock.now()
        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._open_for = min(self.max_open, self._open_for * 2)
            self._trip(now)
            return
        self._add(False)
        if self._state is BreakerState.CLOSED and self._should_trip():
            self._trip(now)

    def abort(self) -> None:
        """The call was cancelled: free the probe slot without counting it either way."""
        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)

    # ---- observation -----------------------------------------------------------------
    def health(self) -> ProviderHealth:
        self._prune(self.clock.now())
        n = len(self._results)
        bad = sum(1 for _, ok in self._results if not ok)
        return ProviderHealth(self.name, self._state, bad / n if n else 0.0, n, self._retry_at)

    @property
    def state(self) -> BreakerState:
        return self._state

    # ---- internals -------------------------------------------------------------------
    def _add(self, ok: bool) -> None:
        now = self.clock.now()
        self._results.append((now, ok))
        self._prune(now)

    def _prune(self, now: datetime) -> None:
        while self._results and now - self._results[0][0] > self.window:
            self._results.popleft()

    def _should_trip(self) -> bool:
        n = len(self._results)
        if n < self.min_samples:
            return False
        bad = sum(1 for _, ok in self._results if not ok)
        return bad / n >= self.threshold

    def _trip(self, now: datetime) -> None:
        self._retry_at = now + timedelta(seconds=self._open_for)
        self._set(BreakerState.OPEN)

    def _set(self, state: BreakerState) -> None:
        if state is not self._state:
            self._state = state
            self.transitions.append((self.clock.now(), state))


class ProtectedProvider:
    """Same interface as a Provider, with place_call behind the breaker. The dialer holds
    one of these and cannot tell the difference (or reach the raw provider)."""

    def __init__(self, inner: Provider, breaker: CircuitBreaker):
        self._inner, self.breaker = inner, breaker
        self.name = inner.name

    async def place_call(self, req: PlaceCallRequest) -> PlacedCall:
        self.breaker.before_call()  # raises CircuitOpenError without touching the provider
        try:
            placed = await self._inner.place_call(req)
        except ProviderRejected:
            self.breaker.record_success()  # provider answered; our request was the problem
            raise
        except Exception:
            self.breaker.record_failure()
            raise
        except BaseException:  # CancelledError: not the provider's fault
            self.breaker.abort()
            raise
        self.breaker.record_success()
        return placed

    async def hangup(self, provider_call_id: str) -> None:
        await self._inner.hangup(provider_call_id)

    async def get_call_status(self, *, provider_call_id: str | None = None,
                              client_ref: int | None = None) -> ProviderCallStatus:
        return await self._inner.get_call_status(provider_call_id=provider_call_id, client_ref=client_ref)

    def set_event_handler(self, handler: EventHandler) -> None:
        self._inner.set_event_handler(handler)

    def health(self) -> ProviderHealth:
        return self.breaker.health()
