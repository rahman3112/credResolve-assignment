"""The schema's own guarantees, checked against real PostgreSQL. These are the backstops
that still hold if application code has a bug."""

from datetime import datetime, timezone

import asyncpg
import pytest

pytestmark = pytest.mark.integration
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


async def seed(conn, n_borrowers=2, n_agents=2):
    cid = await conn.fetchval(
        "INSERT INTO campaigns (name, mode, created_at) VALUES ('c', 'PROGRESSIVE', $1) RETURNING id", NOW)
    b = [await conn.fetchval(
        "INSERT INTO borrowers (campaign_id, phone, next_attempt_at) VALUES ($1, $2, $3) RETURNING id",
        cid, f"+1555000{i}", NOW) for i in range(n_borrowers)]
    a = [await conn.fetchval(
        "INSERT INTO agents (campaign_id, name, state, state_since) VALUES ($1, $2, 'AVAILABLE', $3) RETURNING id",
        cid, f"agent{i}", NOW) for i in range(n_agents)]
    return cid, b, a


async def new_call(conn, cid, borrower, agent=None, state="QUEUED"):
    return await conn.fetchval(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, state, attempt_no, created_at) "
        "VALUES ($1, $2, $3, $4, 1, $5) RETURNING id", cid, borrower, agent, state, NOW)


async def test_borrower_has_at_most_one_non_terminal_call(conn):
    cid, (b1, _), _ = await seed(conn)
    first = await new_call(conn, cid, b1)
    with pytest.raises(asyncpg.UniqueViolationError):
        await new_call(conn, cid, b1)
    # Once the first call is terminal the borrower may be called again (a retry).
    await conn.execute("UPDATE calls SET state='FAILED', end_reason='NO_ANSWER' WHERE id=$1", first)
    await new_call(conn, cid, b1)


@pytest.mark.parametrize("live", ["QUEUED", "RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED"])
async def test_every_non_terminal_state_blocks_a_second_call(conn, live):
    cid, (b1, _), _ = await seed(conn)
    await new_call(conn, cid, b1, state=live)
    with pytest.raises(asyncpg.UniqueViolationError):
        await new_call(conn, cid, b1)


async def test_agent_has_at_most_one_live_call_but_predictive_calls_are_unbounded(conn):
    cid, (b1, b2), (a1, _) = await seed(conn)
    await new_call(conn, cid, b1, agent=a1)
    with pytest.raises(asyncpg.UniqueViolationError):
        await new_call(conn, cid, b2, agent=a1)
    # Calls with no agent yet (predictive, still ringing) are not limited by this index.
    b3 = await conn.fetchval(
        "INSERT INTO borrowers (campaign_id, phone, next_attempt_at) VALUES ($1,'+1','2026-01-01') RETURNING id", cid)
    await new_call(conn, cid, b2, agent=None)
    await new_call(conn, cid, b3, agent=None)


async def test_provider_event_dedup_key(conn):
    cid, (b1, _), _ = await seed(conn)
    call = await new_call(conn, cid, b1)
    ins = ("INSERT INTO provider_events (provider, provider_call_id, event_type, seq, call_id, received_at, outcome) "
           "VALUES ($1, $2, $3, $4, $6, $5, 'APPLIED')")
    await conn.execute(ins, "B", "x1", "ANSWERED", 0, NOW, call)
    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute(ins, "B", "x1", "ANSWERED", 0, NOW, call)
    # Different type, different seq, different provider or different call are all distinct.
    await conn.execute(ins, "B", "x1", "COMPLETED", 0, NOW, call)
    await conn.execute(ins, "B", "x1", "ANSWERED", 1, NOW, call)
    await conn.execute(ins, "A", "x1", "ANSWERED", 0, NOW, call)
    await conn.execute(ins, "B", "x2", "ANSWERED", 0, NOW, call)


async def test_check_constraints_reject_unknown_values(conn):
    cid, (b1, _), (a1, _) = await seed(conn)
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute("UPDATE agents SET state='BUSY' WHERE id=$1", a1)
    with pytest.raises(asyncpg.CheckViolationError):
        await new_call(conn, cid, b1, state="DIALING")
    # A lease on an AVAILABLE agent is a bug (it would hide a leak from the reconciler).
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute("UPDATE agents SET lease_until=$2 WHERE id=$1", a1, NOW)
    # end_reason only makes sense on a finished call.
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute("UPDATE calls SET end_reason='NO_ANSWER' WHERE id=$1",
                           await new_call(conn, cid, b1))


async def test_safety_decision_cannot_approve_more_than_proposed(conn):
    cid, _, _ = await seed(conn)
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            "INSERT INTO safety_decisions (campaign_id, decided_at, proposed_n, approved_n, verdict, reason) "
            "VALUES ($1, $2, 5, 6, 'APPROVE', 'x')", cid, NOW)
