"""Injected time (D-07). Repository functions take `now` as a parameter; whoever drives
them (worker loop, simulator, test) gets it from a Clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class VirtualClock:
    """Only moves when told to. The simulator advances it; no real sleeping anywhere."""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> datetime:
        self._now += timedelta(seconds=seconds)
        return self._now

    def advance_to(self, when: datetime) -> None:
        """Move forward to `when` (never backwards)."""
        if when > self._now:
            self._now = when
