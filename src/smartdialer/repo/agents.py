"""Agent persistence. Every function takes a connection so the caller owns the transaction
boundary; nothing here opens one except where stated."""

from __future__ import annotations

from datetime import datetime

import asyncpg

from smartdialer.repo.models import Agent
from smartdialer.states import AgentState, assert_agent_transition


async def create_agent(conn, campaign_id: int, name: str, now: datetime,
                       state: AgentState = AgentState.OFFLINE) -> Agent:
    r = await conn.fetchrow(
        "INSERT INTO agents (campaign_id, name, state, state_since) VALUES ($1, $2, $3, $4) RETURNING *",
        campaign_id, name, state.value, now)
    return Agent.from_record(r)


async def get_agent(conn, agent_id: int, *, for_update: bool = False) -> Agent | None:
    r = await conn.fetchrow(f"SELECT * FROM agents WHERE id = $1{' FOR UPDATE' if for_update else ''}", agent_id)
    return Agent.from_record(r) if r else None


async def reserve_agent(conn, campaign_id: int, now: datetime, lease_until: datetime) -> Agent | None:
    """RULE 2. Claim the longest-idle AVAILABLE agent in ONE statement.

    Why this cannot double-book:
      * The inner SELECT ... FOR UPDATE SKIP LOCKED takes a row lock on the candidate. A
        second worker running the same statement at the same moment *skips* that locked row
        and takes the next agent instead of waiting (no queueing, no deadlock).
      * If the other worker already committed (row is now RESERVED) by the time we try to
        lock it, PostgreSQL re-checks `state = 'AVAILABLE'` against the new row version and
        drops it. The extra `AND state = 'AVAILABLE'` on the outer UPDATE is belt and braces.
      * Selecting and updating happen in the same statement, so there is no gap between
        "I saw it free" and "I took it" for another worker to slip into. The naive
        SELECT-then-UPDATE is shown to double-book in tests/integration/test_reservation_race.py.

    Returns None when no agent is free; that is normal, not an error.
    """
    r = await conn.fetchrow(
        """
        UPDATE agents
           SET state = 'RESERVED', version = version + 1, state_since = $2, lease_until = $3,
               wrap_up_until = NULL
         WHERE id = (SELECT id FROM agents
                      WHERE campaign_id = $1 AND state = 'AVAILABLE'
                      ORDER BY state_since, id
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
           AND state = 'AVAILABLE'
        RETURNING *
        """,
        campaign_id, now, lease_until)
    return Agent.from_record(r) if r else None


async def transition_agent(conn, agent: Agent, to_state: AgentState, now: datetime, *,
                           lease_until: datetime | None = None,
                           wrap_up_until: datetime | None = None) -> Agent | None:
    """RULE 4: compare-and-set. Applies only if the row is still at the snapshot's version
    AND state. Returns None if we lost the race (caller re-reads and re-decides).

    `lease_until` / `wrap_up_until` are always written, so leaving RESERVED/DIALING clears
    the lease by default (the schema CHECK would reject a stale one anyway).
    """
    assert_agent_transition(agent.state, to_state)  # a bug in *our* code, so raise
    try:
        r = await conn.fetchrow(
            """
            UPDATE agents
               SET state = $4, version = version + 1, state_since = $5,
                   lease_until = $6, wrap_up_until = $7,
               pause_requested = CASE WHEN $4 IN ('PAUSED', 'OFFLINE') THEN false ELSE pause_requested END
             WHERE id = $1 AND version = $2 AND state = $3
            RETURNING *
            """,
            agent.id, agent.version, agent.state.value, to_state.value, now, lease_until, wrap_up_until)
    except asyncpg.CheckViolationError as e:
        raise ValueError(f"invalid lease/wrap-up for {to_state}: {e}") from e
    return Agent.from_record(r) if r else None


async def end_expired_wrap_ups(conn, campaign_id: int, now: datetime) -> int:
    """WRAP_UP -> AVAILABLE (or PAUSED if a break was requested while busy, D-05) for agents
    whose timer elapsed. One set-based statement; it is guarded by `state = 'WRAP_UP'` and
    bumps `version`, so any concurrent CAS on the same agent either happens first and this
    skips it, or loses."""
    r = await conn.fetch(
        """
        UPDATE agents SET state = CASE WHEN pause_requested THEN 'PAUSED' ELSE 'AVAILABLE' END,
                          pause_requested = false, version = version + 1, state_since = $2,
                          wrap_up_until = NULL
         WHERE campaign_id = $1 AND state = 'WRAP_UP' AND wrap_up_until <= $2
        RETURNING id
        """,
        campaign_id, now)
    return len(r)


async def count_by_state(conn, campaign_id: int) -> dict[AgentState, int]:
    rows = await conn.fetch(
        "SELECT state, count(*) AS n FROM agents WHERE campaign_id = $1 GROUP BY state", campaign_id)
    out = {s: 0 for s in AgentState}
    out.update({AgentState(r["state"]): r["n"] for r in rows})
    return out


async def request_pause(conn, agent_id: int, now: datetime) -> str:
    """Agent asks for a break. Idle (AVAILABLE/WRAP_UP): pause now. Busy: remember it and
    apply after wrap-up so the call is never stranded. Returns 'PAUSED', 'DEFERRED' or 'NOOP'."""
    async with conn.transaction():
        agent = await get_agent(conn, agent_id, for_update=True)
        if agent.state in (AgentState.AVAILABLE, AgentState.WRAP_UP):
            moved = await transition_agent(conn, agent, AgentState.PAUSED, now)
            return "PAUSED" if moved else "NOOP"
        if agent.state in (AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED):
            await conn.execute("UPDATE agents SET pause_requested = true, version = version + 1 WHERE id = $1", agent_id)
            return "DEFERRED"
        return "NOOP"
