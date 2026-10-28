"""The headline concurrency tests: many workers, few agents, nobody gets double-booked."""

import asyncio
from datetime import timedelta

import pytest

from smartdialer.repo.agents import count_by_state, get_agent, reserve_agent, transition_agent
from smartdialer.repo.borrowers import reserve_borrower
from smartdialer.repo.reservation import reserve_progressive_call
from smartdialer.states import AgentState

from .helpers import NOW, make_available_agents, make_borrowers, make_campaign

pytestmark = pytest.mark.integration

WORKERS, AGENTS = 50, 10
LEASE = NOW + timedelta(seconds=30)


async def run_workers(pool, fn, n=WORKERS):
    """Start n workers on n separate connections and release them at the same instant."""
    gate = asyncio.Event()

    async def worker(i):
        async with pool.acquire() as conn:
            await gate.wait()
            return await fn(conn, i)

    tasks = [asyncio.create_task(worker(i)) for i in range(n)]
    await asyncio.sleep(0.2)  # let every task acquire its connection and park on the gate
    gate.set()
    return await asyncio.gather(*tasks)


@pytest.mark.parametrize("round_", range(10))  # a race that passes once proves little
async def test_50_workers_10_agents_no_double_booking(pool, round_):
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        await make_available_agents(conn, cid, AGENTS)

    results = await run_workers(pool, lambda conn, i: reserve_agent(conn, cid, NOW, LEASE))

    won = [a for a in results if a is not None]
    assert len(won) == AGENTS  # every agent got used...
    assert len({a.id for a in won}) == AGENTS  # ...by exactly one worker
    assert results.count(None) == WORKERS - AGENTS
    async with pool.acquire() as conn:
        counts = await count_by_state(conn, cid)
    assert counts[AgentState.RESERVED] == AGENTS and counts[AgentState.AVAILABLE] == 0


async def test_reservation_goes_to_the_longest_idle_agent(pool):
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        old = (await make_available_agents(conn, cid, 1, NOW))[0]
        await make_available_agents(conn, cid, 3, NOW + timedelta(seconds=60))
        got = await reserve_agent(conn, cid, NOW + timedelta(seconds=90), LEASE)
    assert got.id == old.id


async def test_progressive_reservation_50_workers_10_agents_100_borrowers(pool):
    """Full reservation (agent + borrower + call row): no agent, borrower or call is shared,
    nothing leaks, and the invariant 'agent-bound calls <= agents' holds."""
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        await make_available_agents(conn, cid, AGENTS)
        await make_borrowers(conn, cid, 100)

    results = await run_workers(pool, lambda conn, i: reserve_progressive_call(conn, cid, NOW, 30))
    won = [r for r in results if r is not None]

    assert len(won) == AGENTS
    assert len({r.agent.id for r in won}) == AGENTS
    assert len({r.borrower.id for r in won}) == AGENTS
    assert len({r.call.id for r in won}) == AGENTS
    async with pool.acquire() as conn:
        assert (await conn.fetchval("SELECT count(*) FROM calls")) == AGENTS
        by_status = dict(await conn.fetch("SELECT status, count(*) FROM borrowers GROUP BY status"))
        assert by_status == {"RESERVED": AGENTS, "PENDING": 90}  # losers leaked nothing


async def test_borrower_contention_5_borrowers_50_agents(pool):
    """Scarce resource is borrowers this time: 50 agents, 5 borrowers, 50 workers."""
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        await make_available_agents(conn, cid, 50)
        await make_borrowers(conn, cid, 5)

    results = await run_workers(pool, lambda conn, i: reserve_progressive_call(conn, cid, NOW, 30))
    won = [r for r in results if r is not None]
    assert len(won) == 5
    assert len({r.borrower.id for r in won}) == 5
    async with pool.acquire() as conn:
        counts = await count_by_state(conn, cid)
        # The 45 workers that found an agent but no borrower rolled their agent back.
        assert counts[AgentState.RESERVED] == 5 and counts[AgentState.AVAILABLE] == 45


async def test_borrower_claims_are_exclusive(pool):
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        await make_borrowers(conn, cid, 7)
    results = await run_workers(pool, lambda conn, i: reserve_borrower(conn, cid, NOW))
    won = [b for b in results if b is not None]
    assert len(won) == 7 and len({b.id for b in won}) == 7


async def test_cas_only_one_of_many_concurrent_transitions_wins(pool):
    """RULE 4: 20 workers hold the same stale snapshot and all try to pause the agent."""
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        (agent,) = await make_available_agents(conn, cid, 1)

    results = await run_workers(
        pool, lambda conn, i: transition_agent(conn, agent, AgentState.PAUSED, NOW), n=20)
    assert sum(r is not None for r in results) == 1
    async with pool.acquire() as conn:
        final = await get_agent(conn, agent.id)
    assert final.state is AgentState.PAUSED and final.version == agent.version + 1


async def test_cas_rejects_a_stale_snapshot(pool):
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        (agent,) = await make_available_agents(conn, cid, 1)
        assert await reserve_agent(conn, cid, NOW, LEASE) is not None  # someone else moved it
        # Our snapshot still says AVAILABLE/old version: the CAS must refuse, not overwrite.
        assert await transition_agent(conn, agent, AgentState.PAUSED, NOW) is None
        assert (await get_agent(conn, agent.id)).state is AgentState.RESERVED


async def test_negative_control_naive_select_then_update_double_books(pool):
    """Proves the race test has teeth. The obvious-but-wrong implementation (read an
    AVAILABLE agent, then update it by id) hands the SAME agent to every worker when they
    interleave. A barrier forces the interleaving deterministically."""
    async with pool.acquire() as conn:
        cid = await make_campaign(conn)
        await make_available_agents(conn, cid, 1)
    barrier = asyncio.Barrier(WORKERS)

    async def naive(conn, i):
        agent_id = await conn.fetchval(
            "SELECT id FROM agents WHERE campaign_id=$1 AND state='AVAILABLE' LIMIT 1", cid)
        await barrier.wait()  # everyone has now "seen" the agent as free
        if agent_id is None:
            return None
        await conn.execute("UPDATE agents SET state='RESERVED' WHERE id=$1", agent_id)
        return agent_id

    results = await run_workers(pool, naive)
    assert len([r for r in results if r is not None]) == WORKERS  # 50 workers, ONE agent
