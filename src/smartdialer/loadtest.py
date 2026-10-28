"""Basic load test: how fast can workers reserve (agent + borrower + call) against PostgreSQL?

python -m smartdialer.loadtest --agents 100 1000 10000 --workers 50 --seconds 10

Unlike the simulator this runs in REAL time: it is measuring the database, so virtual time would
be meaningless. Workers hammer `reserve_progressive_call` (the contended hot path: one atomic
UPDATE ... SKIP LOCKED for the agent, one for the borrower, one INSERT) and immediately free the
agent again so the pool never runs dry, like a floor where calls finish as fast as they start.

Reported per agent count:
  * reservations/sec and latency percentiles of the reservation transaction (p99 is the number
    that tells you about contention) and of the full reserve+release cycle;
  * the per-tick COST OF DECIDING (load_snapshot, which the pacer and Safety Controller both run),
    which grows with the number of agents rows it aggregates. At large scale this, not the
    reservation, is what hits a ceiling first (see the scale section of the docs);
  * double-booking violations observed in memory (an agent reserved while another worker still
    held it) and invariant checks against the database. Both must be zero.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from datetime import datetime, timezone

import asyncpg

from smartdialer import db
from smartdialer.repo.campaigns import create_campaign
from smartdialer.repo.models import Mode
from smartdialer.repo.reservation import reserve_progressive_call
from smartdialer.repo.snapshot import load_snapshot
from smartdialer.sim.dbutil import ensure_sim_database

RELEASE_SQL = (
    "UPDATE calls SET state = 'COMPLETED', ended_at = $2, version = version + 1, lease_until = NULL WHERE id = $1",
    "UPDATE agents SET state = 'AVAILABLE', state_since = $2, lease_until = NULL, version = version + 1 WHERE id = $1",
    "UPDATE borrowers SET status = 'PENDING', next_attempt_at = $2, version = version + 1 WHERE id = $1",
)


def percentile(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return float("nan")
    return sorted_ms[min(len(sorted_ms) - 1, int(p / 100 * len(sorted_ms)))]


async def _seed(pool, agents: int, borrowers: int):
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        camp = await create_campaign(conn, "load", Mode.PROGRESSIVE, now)
        await conn.execute(
            "INSERT INTO agents (campaign_id, name, state, state_since) "
            "SELECT $1, 'a' || g, 'AVAILABLE', $2 FROM generate_series(1, $3) g", camp.id, now, agents)
        await conn.execute(
            "INSERT INTO borrowers (campaign_id, phone, next_attempt_at) "
            "SELECT $1, '+1' || lpad(g::text, 9, '0'), $2 FROM generate_series(1, $3) g", camp.id, now, borrowers)
    return camp.id


async def _snapshot_cost(pool, cid: int, n: int = 30) -> dict:
    samples = []
    async with pool.acquire() as conn:
        for _ in range(n):
            t0 = time.perf_counter()
            await load_snapshot(conn, cid, datetime.now(timezone.utc))
            samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    return {"p50_ms": round(statistics.median(samples), 2), "p99_ms": round(percentile(samples, 99), 2)}


async def run_load(agents: int, workers: int, seconds: float, db_name: str = "dialer_load_sim") -> dict:
    dsn = await ensure_sim_database(db_name)
    admin = await asyncpg.connect(dsn)
    try:
        await db.reset_schema(admin)
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(dsn, min_size=workers + 2, max_size=workers + 2)
    try:
        cid = await _seed(pool, agents, borrowers=agents * 2 + 100)
        fresh = await _snapshot_cost(pool, cid)

        held: set[int] = set()  # agents some worker currently holds: the in-memory double-booking detector
        violations = 0
        reserve_ms: list[float] = []
        cycle_ms: list[float] = []
        idle_polls = 0
        deadline = time.perf_counter() + seconds

        async def worker():
            nonlocal violations, idle_polls
            async with pool.acquire() as conn:
                while time.perf_counter() < deadline:
                    now = datetime.now(timezone.utc)
                    t0 = time.perf_counter()
                    res = await reserve_progressive_call(conn, cid, now, 60)
                    t1 = time.perf_counter()
                    if res is None:
                        idle_polls += 1
                        await asyncio.sleep(0.001)
                        continue
                    if res.agent.id in held:
                        violations += 1
                    held.add(res.agent.id)
                    async with conn.transaction():
                        await conn.execute(RELEASE_SQL[0], res.call.id, now)
                        await conn.execute(RELEASE_SQL[2], res.borrower.id, now)
                        held.discard(res.agent.id)  # discard before the agent is visible as AVAILABLE again
                        await conn.execute(RELEASE_SQL[1], res.agent.id, now)
                    t2 = time.perf_counter()
                    reserve_ms.append((t1 - t0) * 1000)
                    cycle_ms.append((t2 - t0) * 1000)

        t_start = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(workers)))
        elapsed = time.perf_counter() - t_start

        async with pool.acquire() as conn:
            inv = await conn.fetchrow(
                "SELECT (SELECT count(*) FROM agents WHERE state IN ('RESERVED','DIALING','CONNECTED')) AS busy, "
                "(SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')) AS live, "
                "(SELECT count(*) FROM borrowers WHERE status = 'RESERVED') AS reserved_borrowers, "
                "(SELECT count(*) FROM calls) AS total_calls")
        after = await _snapshot_cost(pool, cid)
        reserve_ms.sort()
        cycle_ms.sort()
        return {
            "agents": agents, "workers": workers, "seconds": round(elapsed, 1),
            "reservations": len(reserve_ms), "reservations_per_sec": round(len(reserve_ms) / elapsed, 1),
            "reserve_ms": {f"p{p}": round(percentile(reserve_ms, p), 2) for p in (50, 95, 99)}
                          | {"max": round(reserve_ms[-1], 2) if reserve_ms else None},
            "cycle_ms": {f"p{p}": round(percentile(cycle_ms, p), 2) for p in (50, 95, 99)},
            "idle_polls": idle_polls,
            "snapshot_fresh": fresh, "snapshot_after_load": after, "calls_rows_after": inv["total_calls"],
            "double_booking_violations": violations,
            "invariants_ok": inv["busy"] == 0 and inv["live"] == 0 and inv["reserved_borrowers"] == 0,
        }
    finally:
        await pool.close()


def table(results: list[dict]) -> str:
    head = ["agents", "workers", "reservations", "res/s", "p50 ms", "p95 ms", "p99 ms", "max ms", "cycle p99 ms",
            "snapshot p50/p99 ms", "violations", "invariants"]
    rows = [[str(r["agents"]), str(r["workers"]), str(r["reservations"]), str(r["reservations_per_sec"]),
             str(r["reserve_ms"]["p50"]), str(r["reserve_ms"]["p95"]), str(r["reserve_ms"]["p99"]),
             str(r["reserve_ms"]["max"]), str(r["cycle_ms"]["p99"]),
             f"{r['snapshot_fresh']['p50_ms']}/{r['snapshot_fresh']['p99_ms']}",
             str(r["double_booking_violations"]), "ok" if r["invariants_ok"] else "BROKEN"] for r in results]
    w = [max(len(head[i]), *(len(r[i]) for r in rows)) for i in range(len(head))]
    line = lambda c: "| " + " | ".join(x.rjust(n) for x, n in zip(c, w)) + " |"
    return "\n".join([line(head), "|" + "|".join("-" * (n + 2) for n in w) + "|", *map(line, rows)])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m smartdialer.loadtest", description=__doc__.split("\n\n")[0])
    ap.add_argument("--agents", type=int, nargs="+", default=[100, 1000, 10000])
    ap.add_argument("--workers", type=int, default=50)
    ap.add_argument("--seconds", type=float, default=10.0)
    ap.add_argument("--db-name", default="dialer_load_sim", help="throwaway database (must end in _sim)")
    ap.add_argument("--json", help="also write results here")
    args = ap.parse_args(argv)
    results = []
    for n in args.agents:
        print(f"load test: {n} agents, {args.workers} workers, {args.seconds}s ...", file=sys.stderr)
        results.append(asyncio.run(run_load(n, args.workers, args.seconds, args.db_name)))
    print(table(results))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=1)
    return 0 if all(r["invariants_ok"] and not r["double_booking_violations"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
