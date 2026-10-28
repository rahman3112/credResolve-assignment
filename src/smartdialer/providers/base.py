"""The only thing the dialer knows about a telecom provider (Phase 3).

Provider contract, which both mocks (and a real Plivo adapter) must honour:
  * place_call is IDEMPOTENT on `client_ref`: asking twice with the same client_ref returns
    the same provider_call_id and never rings the borrower twice. This is what makes it safe
    to retry after a timeout, where we cannot know whether the call was actually placed.
  * Events are at-least-once, possibly duplicated, possibly out of order, possibly dropped.
    Every event echoes `client_ref`. The dialer never trusts delivery; it dedups (events.py)
    and reconciles via get_call_status.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from smartdialer.repo.events import ProviderEvent


class ProviderError(Exception):
    """The provider answered with an error: the call was NOT placed (HTTP 5xx and the like).
    Counts against the provider's health. A timeout is the ambiguous case, see below."""


class ProviderTimeout(ProviderError):
    """No answer in time. AMBIGUOUS: the call may or may not have been placed."""


class ProviderRejected(Exception):
    """Permanent, per-request refusal (invalid number). The provider is healthy; the
    request is bad. Not a health signal and not worth retrying."""


class CircuitOpenError(ProviderError):
    """Failed fast by our own circuit breaker: the provider was never contacted."""


@dataclass(frozen=True, slots=True)
class PlaceCallRequest:
    client_ref: int  # our calls.id
    phone: str


@dataclass(frozen=True, slots=True)
class PlacedCall:
    provider_call_id: str


class ProviderCallState(enum.StrEnum):
    """What the provider currently believes (not what we have seen events for)."""
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"  # provider has no such call: it was never placed


@dataclass(frozen=True, slots=True)
class ProviderCallStatus:
    state: ProviderCallState
    provider_call_id: str | None = None
    client_ref: int | None = None
    reason: str | None = None  # for FAILED: NO_ANSWER, BUSY, CANCELLED, ...
    was_answered: bool = False  # True even after COMPLETED: "did the borrower ever pick up?"


EventHandler = Callable[[ProviderEvent], Awaitable[None]]


class Provider(Protocol):
    name: str

    async def place_call(self, req: PlaceCallRequest) -> PlacedCall: ...

    async def hangup(self, provider_call_id: str) -> None:
        """Idempotent; hanging up a finished or unknown call is a no-op."""

    async def get_call_status(self, *, provider_call_id: str | None = None,
                              client_ref: int | None = None) -> ProviderCallStatus:
        """Lookup by either id. By client_ref is how we recover when place_call timed out or
        the worker died before saving provider_call_id."""

    def set_event_handler(self, handler: EventHandler) -> None: ...
