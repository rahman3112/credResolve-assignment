"""Provider callbacks -> call state, exactly once, in one transaction (RULE 5).

Providers are untrusted: they duplicate, reorder, delay and sometimes lie. This module is
the only door through which a provider event can touch a call.
"""

from __future__ import annotations

import enum
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from smartdialer.repo.calls import transition_call
from smartdialer.repo.models import Call
from smartdialer.states import CallDecision, CallState, decide_call_transition


class ProviderEventType(enum.StrEnum):
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"  # the call ended normally
    FAILED = "FAILED"  # busy / no-answer / network error; `reason` says which


class EventOutcome(enum.StrEnum):
    APPLIED = "APPLIED"  # first sighting, moved the call forward
    DUPLICATE = "DUPLICATE"  # seen before (dedup key) or already in that state: no-op
    STALE = "STALE"  # arrived behind the call's progress: stored as a fact only
    ILLEGAL = "ILLEGAL"  # forward but not an edge: stored, logged, ignored
    UNKNOWN_CALL = "UNKNOWN_CALL"  # cannot tell which call this is: NOT stored (see below)


@dataclass(frozen=True, slots=True)
class ProviderEvent:
    provider: str
    provider_call_id: str
    type: ProviderEventType
    seq: int = 0
    # Our calls.id, which we sent to the provider as a client reference. Lets us resolve an
    # event that beats place_call()'s response back to us, or a crash before we saved
    # provider_call_id.
    client_ref: int | None = None
    occurred_at: datetime | None = None
    reason: str | None = None
    payload: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventResult:
    outcome: EventOutcome
    call: Call | None  # the call after the event (unchanged unless APPLIED)


# on_applied(conn, call_before, call_after) runs INSIDE the event transaction, so side
# effects like "release the agent when the call ends" commit or roll back together with the
# event row and the state change.
OnApplied = Callable[[object, Call, Call], Awaitable[None]]


def _target(ev: ProviderEvent) -> tuple[CallState, str | None]:
    match ev.type:
        case ProviderEventType.RINGING:
            return CallState.RINGING, None
        case ProviderEventType.ANSWERED:
            return CallState.ANSWERED, None
        case ProviderEventType.COMPLETED:
            return CallState.COMPLETED, None
        case ProviderEventType.FAILED:
            return CallState.FAILED, ev.reason or "PROVIDER_ERROR"
    raise ValueError(ev.type)


async def _resolve_and_lock(conn, ev: ProviderEvent) -> Call | None:
    """Find the call and take a row lock on it for the rest of the transaction.

    The lock serialises concurrent events for the SAME call (two copies of ANSWERED handled
    by two workers) while leaving different calls fully parallel."""
    r = await conn.fetchrow(
        "SELECT * FROM calls WHERE provider = $1 AND provider_call_id = $2 FOR UPDATE",
        ev.provider, ev.provider_call_id)
    if r is None and ev.client_ref is not None:
        r = await conn.fetchrow(
            "SELECT * FROM calls WHERE id = $1 AND (provider IS NULL OR provider = $2) FOR UPDATE",
            ev.client_ref, ev.provider)
        if r is not None and r["provider_call_id"] is None:
            # First time we hear this provider id (crash or race before we saved it): adopt.
            r = await conn.fetchrow(
                "UPDATE calls SET provider = $2, provider_call_id = $3, version = version + 1 "
                "WHERE id = $1 RETURNING *", r["id"], ev.provider, ev.provider_call_id)
    return Call.from_record(r) if r else None


async def apply_provider_event(conn, ev: ProviderEvent, now: datetime,
                               on_applied: OnApplied | None = None, *,
                               lease_seconds: float | None = None) -> EventResult:
    """Record `ev` and, if it is news, advance the call. Safe to call any number of times,
    in any order, from any number of workers, with the same event.

    Everything below is ONE transaction: event row, outcome, state change and the
    on_applied side effects. A crash anywhere leaves either all of it or none of it, so
    "event stored but call not moved" cannot exist (and the provider's redelivery, or the
    reconciler, simply repeats the work).

    `lease_seconds`: progress is proof of life, so a non-terminal move renews the call's lease.

    Events we cannot attach to a call are not stored: the dedup row would swallow the
    provider's redelivery, which may be the only copy we ever get that we CAN attach.
    """
    async with conn.transaction():
        call = await _resolve_and_lock(conn, ev)
        if call is None:
            return EventResult(EventOutcome.UNKNOWN_CALL, None)

        # Dedup (RULE 5). DO NOTHING + RETURNING gives a row only for the first sighting.
        # A concurrent twin blocks on the unique index until we commit, then sees the
        # conflict. (It is also blocked by the call lock above, so this is belt and braces.)
        first = await conn.fetchval(
            """
            INSERT INTO provider_events (provider, provider_call_id, event_type, seq, call_id,
                                         occurred_at, received_at, payload, outcome)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'APPLIED')
            ON CONFLICT (provider, provider_call_id, event_type, seq) DO NOTHING
            RETURNING id
            """,
            ev.provider, ev.provider_call_id, ev.type.value, ev.seq, call.id,
            ev.occurred_at, now, json.dumps(ev.payload))
        if first is None:
            await conn.execute(
                "UPDATE provider_events SET duplicate_count = duplicate_count + 1 "
                "WHERE provider = $1 AND provider_call_id = $2 AND event_type = $3 AND seq = $4",
                ev.provider, ev.provider_call_id, ev.type.value, ev.seq)
            return EventResult(EventOutcome.DUPLICATE, call)

        target, end_reason = _target(ev)
        decision = decide_call_transition(call.state, target)
        after = call
        if decision is CallDecision.APPLY:
            terminal = target in (CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED)
            moved = await transition_call(
                conn, call, target, now, end_reason=end_reason,
                lease_until=None if terminal else (
                    now + timedelta(seconds=lease_seconds) if lease_seconds else call.lease_until))
            if moved is None:  # impossible while we hold the row lock; fail loudly if not
                raise RuntimeError(f"CAS lost on locked call {call.id}")
            after = moved
            outcome = EventOutcome.APPLIED
        else:
            # STALE / DUPLICATE (same state, different seq) / ILLEGAL: keep the fact only.
            outcome = EventOutcome(decision.value)

        if outcome is not EventOutcome.APPLIED:
            await conn.execute("UPDATE provider_events SET outcome = $2 WHERE id = $1", first, outcome.value)
        elif on_applied is not None:
            await on_applied(conn, call, after)
        return EventResult(outcome, after)
