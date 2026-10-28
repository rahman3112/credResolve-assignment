from __future__ import annotations

import json
from datetime import datetime


async def log_decision(conn, *, campaign_id: int, now: datetime, proposed_n: int, approved_n: int,
                       verdict: str, reason: str, inputs: dict) -> int:
    return await conn.fetchval(
        "INSERT INTO safety_decisions (campaign_id, decided_at, proposed_n, approved_n, verdict, reason, inputs) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) RETURNING id",
        campaign_id, now, proposed_n, approved_n, verdict, reason, json.dumps(inputs, default=str))
