"""A complete little dialer world on the virtual clock and real PostgreSQL, for end-to-end tests."""

from __future__ import annotations

import dataclasses
import random
from dataclasses import dataclass, field

from smartdialer.clock import VirtualClock
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.config import PacerConfig, SafetyConfig
from smartdialer.dialer.pacing import PredictivePacer, ProgressivePacer
from smartdialer.dialer.reconciler import Reconciler
from smartdialer.dialer.safety import SafetyController
from smartdialer.dialer.worker import DialerWorker
from smartdialer.providers.behavior import CallBehavior
from smartdialer.providers.breaker import CircuitBreaker, ProtectedProvider
from smartdialer.providers.mock import PROFILE_A, PROFILE_B, MockProvider
from smartdialer.repo.agents import create_agent, transition_agent
from smartdialer.repo.borrowers import create_borrower
from smartdialer.repo.campaigns import create_campaign
from smartdialer.repo.models import Mode
from smartdialer.scheduler import Scheduler
from smartdialer.states import AgentState

TERMINAL_SQL = "('COMPLETED', 'FAILED', 'CANCELLED')"


@dataclass
class World:
    pool: object
    sched: Scheduler
    cid: int
    raw: MockProvider
    provider: ProtectedProvider
    workers: list[DialerWorker]
    behavior: CallBehavior
    agent_ids: list[int] = field(default_factory=list)
    borrower_ids: list[int] = field(default_factory=list)
    reconciler: Reconciler | None = None
    auto_reconcile: bool = False
    t: int = 0

    @property
    def worker(self) -> DialerWorker:
        return self.workers[0]

    async def step(self, seconds: float = 1.0) -> None:
        """One virtual second: every worker ticks (concurrently), then time moves on."""
        import asyncio
        await asyncio.gather(*(w.tick() for w in self.workers))
        self.t += 1
        if self.auto_reconcile and self.t % 5 == 0:  # the reconcile loop, as a background task
            self.sched.spawn(self.reconciler.run_once(self.cid))
        await self.sched.advance(seconds)

    async def run(self, seconds: int, *, check=True) -> None:
        for _ in range(seconds):
            await self.step()
            if check:
                await self.assert_invariants()

    async def run_until(self, cond, max_seconds: int = 3000, *, check=True) -> int:
        for i in range(max_seconds):
            if await cond():
                return i
            await self.step()
            if check:
                await self.assert_invariants()
        raise AssertionError(f"condition not reached in {max_seconds} virtual seconds")

    async def quiesce(self, seconds: int = 600) -> None:
        """Stop dialing (campaign PAUSED) and let everything in flight finish."""
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE campaigns SET status = 'PAUSED' WHERE id = $1", self.cid)
        for _ in range(seconds):
            await self.step()

    async def seed_history(self, *, answered=0, unanswered=0, abandoned=0, age=30.0, setup=10.0) -> None:
        """Insert finished calls as if they had happened `age` seconds ago, so a test can put the
        controller in a known statistical situation without simulating minutes of dialing.
        `abandoned` calls are answered calls that ended ABANDONED (they count as answered too)."""
        now = self.sched.now()
        at = now - __import__("datetime").timedelta(seconds=age)
        async with self.pool.acquire() as conn:
            b = await conn.fetchval(
                "INSERT INTO borrowers (campaign_id, phone, status, next_attempt_at) VALUES ($1, '+history', 'DONE', $2) RETURNING id",
                self.cid, now)
            q = ("INSERT INTO calls (campaign_id, borrower_id, state, attempt_no, created_at, initiated_at, answered_at, ended_at, end_reason) "
                 "SELECT $1::bigint, $2::bigint, $3::text, 1, $4::timestamptz, $4::timestamptz - make_interval(secs => $5::float8), "
                 "$6::timestamptz, $4::timestamptz, $7::text FROM generate_series(1, $8::int)")
            ans_at = at
            if answered:
                await conn.execute(q, self.cid, b, "COMPLETED", at, setup, ans_at, None, answered)
            if abandoned:
                await conn.execute(q, self.cid, b, "FAILED", at, setup, ans_at, "ABANDONED", abandoned)
            if unanswered:
                await conn.execute(q, self.cid, b, "FAILED", at, setup, None, "NO_ANSWER", unanswered)

    async def seed_ringing(self, n: int, age: float = 0.0) -> list[int]:
        """Insert n agentless calls that are ringing right now (a predictive campaign mid-flight)."""
        now = self.sched.now()
        started = now - __import__("datetime").timedelta(seconds=age)
        ids = []
        async with self.pool.acquire() as conn:
            for i in range(n):
                b = await conn.fetchval(
                    "INSERT INTO borrowers (campaign_id, phone, status, attempts, next_attempt_at) "
                    "VALUES ($1, $2, 'RESERVED', 1, $3) RETURNING id", self.cid, f"+ring{i:05d}", now)
                ids.append(await conn.fetchval(
                    "INSERT INTO calls (campaign_id, borrower_id, state, attempt_no, created_at, initiated_at, ringing_at, lease_until) "
                    "VALUES ($1, $2, 'RINGING', 1, $3, $3, $3, $4) RETURNING id",
                    self.cid, b, started + __import__("datetime").timedelta(seconds=i * 0.001),
                    now + __import__("datetime").timedelta(seconds=600)))
        return ids

    async def reconcile(self, *, locked: bool = True):
        """One reconciler pass, run to completion (provider calls take virtual time)."""
        fn = self.reconciler.run_once if locked else self.reconciler.run_unlocked
        return await self.sched.run(fn(self.cid))

    # ---- queries ---------------------------------------------------------------------
    async def one(self, sql, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def rows(self, sql, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def agent_state(self, agent_id) -> AgentState:
        return AgentState(await self.one("SELECT state FROM agents WHERE id = $1", agent_id))

    async def calls(self, **where):
        return await self.rows("SELECT * FROM calls WHERE campaign_id = $1 ORDER BY id", self.cid)

    async def borrower(self, borrower_id):
        return await self.rows_one("SELECT * FROM borrowers WHERE id = $1", borrower_id)

    async def rows_one(self, sql, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def assert_invariants(self) -> dict:
        """True at every commit boundary, i.e. whenever we look:
          * an agent is RESERVED/DIALING/CONNECTED  <=>  it holds exactly one live call
            (=> agent-bound calls can never exceed agents);
          * a borrower is RESERVED  <=>  it has a live call (nobody is dialed twice, nobody leaks)."""
        r = await self.rows_one(
            f"""
            SELECT
              (SELECT count(*) FROM agents WHERE campaign_id = $1
                  AND state IN ('RESERVED', 'DIALING', 'CONNECTED')) AS busy_agents,
              (SELECT count(*) FROM calls WHERE campaign_id = $1 AND agent_id IS NOT NULL
                  AND state NOT IN {TERMINAL_SQL}) AS bound_calls,
              (SELECT count(*) FROM agents WHERE campaign_id = $1) AS agents,
              (SELECT count(*) FROM borrowers WHERE campaign_id = $1 AND status = 'RESERVED') AS reserved_borrowers,
              (SELECT count(*) FROM calls WHERE campaign_id = $1 AND state NOT IN {TERMINAL_SQL}) AS live_calls
            """, self.cid)
        assert r["busy_agents"] == r["bound_calls"], dict(r)
        assert r["bound_calls"] <= r["agents"], dict(r)
        assert r["reserved_borrowers"] == r["live_calls"], dict(r)
        return dict(r)


async def build(pool, *, agents=1, borrowers=1, answer_rate=1.0, talk=20.0, provider="A", profile=None,
                breaker_kw=None, cfg=None, max_attempts=3, wrap_up=30, workers=1, seed=1,
                phones=None, mode=Mode.PROGRESSIVE, online=True, auto_reconcile=False,
                pacer_cfg: PacerConfig | None = None, safety_cfg: SafetyConfig | None = None,
                max_abandon_rate: float = 0.03, provider_name: str | None = None) -> World:
    sched = Scheduler(VirtualClock())
    now = sched.now()
    cfg = cfg or DialerConfig()
    behavior = CallBehavior(answer_rate=answer_rate, mean_talk_seconds=talk)
    if profile is None:
        profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0) if provider == "A" else PROFILE_B
    pname = provider_name or provider  # distinct names keep mock call ids unique across worlds sharing a DB
    raw = MockProvider(pname, profile, behavior, sched, random.Random(seed))
    breaker = CircuitBreaker(pname, sched.clock, **(breaker_kw or {}))
    protected = ProtectedProvider(raw, breaker)

    async with pool.acquire() as conn:
        camp = await create_campaign(conn, "test", mode, now, wrap_up_seconds=wrap_up, max_attempts=max_attempts,
                                     max_abandon_rate=max_abandon_rate)
        agent_ids = []
        for i in range(agents):
            a = await create_agent(conn, camp.id, f"agent{i}", now)
            if online:
                a = await transition_agent(conn, a, AgentState.AVAILABLE, now)
            agent_ids.append(a.id)
        borrower_ids = []
        for i in range(borrowers):
            phone = phones[i] if phones else f"+1555{i:07d}"
            borrower_ids.append((await create_borrower(conn, camp.id, phone, now)).id)

    ws = []
    for k in range(workers):
        safety = SafetyController(pool, protected.health, permit_ttl_seconds=cfg.permit_ttl_seconds, cfg=safety_cfg)
        pacer = PredictivePacer(pacer_cfg) if mode is Mode.PREDICTIVE else ProgressivePacer()
        ws.append(DialerWorker(pool=pool, sched=sched, campaign_id=camp.id, provider=protected,
                               pacer=pacer, safety=safety, cfg=cfg, name=f"w{k + 1}"))
    raw.set_event_handler(ws[0].events.handle)  # any worker can serve any event: all state is in the DB
    reconciler = Reconciler(pool, sched, protected, cfg, processor=ws[0].events)
    return World(pool, sched, camp.id, raw, protected, ws, behavior, agent_ids, borrower_ids,
                 reconciler=reconciler, auto_reconcile=auto_reconcile)


async def dialing_call(w: World) -> int:
    """Reserve one call and commit the intent (INITIATED / agent DIALING) WITHOUT contacting the
    provider: the exact state a worker leaves behind if it dies right after step 2."""
    from smartdialer.repo.reservation import reserve_progressive_call
    async with w.pool.acquire() as conn:
        res = await reserve_progressive_call(conn, w.cid, w.sched.now(), w.worker.cfg.lease_seconds)
    assert res is not None and await w.worker.allocator._intend(res)
    return res.call.id
