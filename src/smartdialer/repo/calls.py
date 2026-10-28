from __future__ import annotations

from datetime import datetime

from smartdialer.repo.models import Call
from smartdialer.states import (
    NON_TERMINAL_CALL_STATES,
    TERMINAL_CALL_STATES,
    CallState,
    assert_call_transition,
)


async def insert_call(conn, *, campaign_id: int, borrower_id: int, agent_id: int | None,
                      state: CallState, attempt_no: int, now: datetime,
                      lease_until: datetime | None) -> Call:
    """Raises asyncpg.UniqueViolationError if the borrower (or agent) already has a live
    call: the schema backstop for borrower/agent allocation."""
    r = await conn.fetchrow(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, state, attempt_no, lease_until, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *",
        campaign_id, borrower_id, agent_id, state.value, attempt_no, lease_until, now)
    return Call.from_record(r)


async def get_call(conn, call_id: int, *, for_update: bool = False) -> Call | None:
    r = await conn.fetchrow(f"SELECT * FROM calls WHERE id = $1{' FOR UPDATE' if for_update else ''}", call_id)
    return Call.from_record(r) if r else None


async def transition_call(conn, call: Call, to_state: CallState, now: datetime, *,
                          lease_until: datetime | None = None,
                          end_reason: str | None = None,
                          agent_id: int | None = None,
                          provider: str | None = None,
                          provider_call_id: str | None = None) -> Call | None:
    """RULE 4: CAS on (id, version, state). None means we lost the race.

    * `lease_until` is always written: pass the new deadline for a live call, nothing for a
      terminal one (a finished call has no owner to expire).
    * agent_id / provider / provider_call_id are only ever *set*, never cleared (COALESCE):
      they are facts about the call's history.
    * Milestone timestamps are filled once (COALESCE) and never overwritten, so a skipped
      state (INITIATED -> ANSWERED) simply leaves ringing_at NULL.
    """
    assert_call_transition(call.state, to_state)
    r = await conn.fetchrow(
        """
        UPDATE calls SET
            state = $4, version = version + 1, lease_until = $5, end_reason = COALESCE($6, end_reason),
            agent_id = COALESCE($7, agent_id), provider = COALESCE($8, provider),
            provider_call_id = COALESCE($9, provider_call_id),
            initiated_at = COALESCE(initiated_at, CASE WHEN $4 = 'INITIATED' THEN $10::timestamptz END),
            ringing_at   = COALESCE(ringing_at,   CASE WHEN $4 = 'RINGING'   THEN $10::timestamptz END),
            answered_at  = COALESCE(answered_at,  CASE WHEN $4 = 'ANSWERED'  THEN $10::timestamptz END),
            connected_at = COALESCE(connected_at, CASE WHEN $4 = 'CONNECTED' THEN $10::timestamptz END),
            ended_at     = COALESCE(ended_at,     CASE WHEN $11 THEN $10::timestamptz END)
         WHERE id = $1 AND version = $2 AND state = $3
        RETURNING *
        """,
        call.id, call.version, call.state.value, to_state.value, lease_until, end_reason,
        agent_id, provider, provider_call_id, now, to_state in TERMINAL_CALL_STATES)
    return Call.from_record(r) if r else None


async def count_live_calls(conn, campaign_id: int) -> int:
    return await conn.fetchval(
        "SELECT count(*) FROM calls WHERE campaign_id = $1 AND state = ANY($2::text[])",
        campaign_id, [s.value for s in NON_TERMINAL_CALL_STATES])


async def live_call_for_agent(conn, agent_id: int, *, for_update: bool = False) -> Call | None:
    """The agent's one non-terminal call, if any (calls_one_active_per_agent guarantees <= 1)."""
    r = await conn.fetchrow(
        "SELECT * FROM calls WHERE agent_id = $1 AND state = ANY($2::text[])"
        + (" FOR UPDATE" if for_update else ""),
        agent_id, [s.value for s in NON_TERMINAL_CALL_STATES])
    return Call.from_record(r) if r else None


async def agent_has_other_live_call(conn, agent_id: int, except_call_id: int) -> bool:
    return bool(await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM calls WHERE agent_id = $1 AND id <> $2 AND state = ANY($3::text[]))",
        agent_id, except_call_id, [s.value for s in NON_TERMINAL_CALL_STATES]))


async def set_provider_id(conn, call_id: int, provider: str, provider_call_id: str) -> Call | None:
    """Record the provider's id for our call. No-op (returns None) if already recorded, e.g.
    an event carrying our client_ref got there first (events.py adopts it)."""
    r = await conn.fetchrow(
        "UPDATE calls SET provider = $2, provider_call_id = $3, version = version + 1 "
        "WHERE id = $1 AND provider_call_id IS NULL RETURNING *", call_id, provider, provider_call_id)
    return Call.from_record(r) if r else None


async def renew_call_lease(conn, call: Call, lease_until: datetime) -> Call | None:
    """CAS: push the lease out. None if the call changed since `call` was read."""
    r = await conn.fetchrow(
        "UPDATE calls SET lease_until = $3, version = version + 1 "
        "WHERE id = $1 AND version = $2 AND state = $4 RETURNING *",
        call.id, call.version, lease_until, call.state.value)
    return Call.from_record(r) if r else None


async def expired_calls(conn, campaign_id: int, now: datetime, limit: int) -> list[Call]:
    """Live calls whose lease has run out, oldest first (served by calls_lease_idx)."""
    rows = await conn.fetch(
        "SELECT * FROM calls WHERE campaign_id = $1 AND lease_until <= $2 AND state = ANY($3::text[]) "
        "ORDER BY lease_until LIMIT $4",
        campaign_id, now, [s.value for s in NON_TERMINAL_CALL_STATES], limit)
    return [Call.from_record(r) for r in rows]
