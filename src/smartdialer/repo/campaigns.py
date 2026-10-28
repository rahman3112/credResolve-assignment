from __future__ import annotations

from datetime import datetime

from smartdialer.repo.models import Campaign, Mode


async def create_campaign(conn, name: str, mode: Mode, now: datetime, *, max_abandon_rate: float = 0.03,
                          wrap_up_seconds: int = 30, max_attempts: int = 3) -> Campaign:
    r = await conn.fetchrow(
        "INSERT INTO campaigns (name, mode, max_abandon_rate, wrap_up_seconds, max_attempts, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
        name, mode.value, max_abandon_rate, wrap_up_seconds, max_attempts, now)
    return Campaign.from_record(r)


async def get_campaign(conn, campaign_id: int) -> Campaign:
    return Campaign.from_record(await conn.fetchrow("SELECT * FROM campaigns WHERE id = $1", campaign_id))


async def set_campaign_status(conn, campaign_id: int, status: str) -> None:
    await conn.execute("UPDATE campaigns SET status = $2 WHERE id = $1", campaign_id, status)
