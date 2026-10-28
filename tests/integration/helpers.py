from __future__ import annotations

from datetime import datetime, timezone

from smartdialer.repo.agents import create_agent, transition_agent
from smartdialer.repo.borrowers import create_borrower
from smartdialer.states import AgentState

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


async def make_campaign(conn, mode="PROGRESSIVE", max_attempts=3) -> int:
    return await conn.fetchval(
        "INSERT INTO campaigns (name, mode, max_attempts, created_at) VALUES ('c', $1, $2, $3) RETURNING id",
        mode, max_attempts, NOW)


async def make_available_agents(conn, campaign_id: int, n: int, now: datetime = NOW):
    agents = []
    for i in range(n):
        a = await create_agent(conn, campaign_id, f"agent{i}", now)
        agents.append(await transition_agent(conn, a, AgentState.AVAILABLE, now))
    return agents


async def make_borrowers(conn, campaign_id: int, n: int, now: datetime = NOW):
    return [await create_borrower(conn, campaign_id, f"+1555{i:07d}", now) for i in range(n)]
