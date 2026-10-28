"""Discrete-event simulation of one campaign: virtual clock, seeded randomness, real PostgreSQL.

What is simulated vs real
  * simulated: time (VirtualClock; no sleeping anywhere), borrowers (answer / talk behaviour),
    the telecom providers (mock A or B), incidents (outages, agents vanishing)
  * REAL: the dialer itself: the pacer, the Safety Controller, the allocator, the event handler,
    the reconciler and every SQL statement, run against PostgreSQL exactly as in production.
That is the point: the simulator exercises the same code paths the tests and the prototype use,
so a result is evidence about the system and not about a model of it.

One virtual second = one worker tick (reserve/dial) + all timers due in that second (provider
events, ring timeouts, wrap-up expiry) + one reconciler pass every 5 s.
"""

from __future__ import annotations

import dataclasses
import random
import time
from dataclasses import dataclass, field
from datetime import timedelta

import asyncpg

from smartdialer import db
from smartdialer.clock import VirtualClock
from smartdialer.dialer.config import DialerConfig, SafetyConfig
from smartdialer.dialer.pacing import PredictivePacer, ProgressivePacer
from smartdialer.dialer.reconciler import ReconcileReport, Reconciler
from smartdialer.dialer.safety import PermitKind, SafetyController
from smartdialer.dialer.worker import DialerWorker
from smartdialer.providers.behavior import CallBehavior
from smartdialer.providers.breaker import CircuitBreaker, ProtectedProvider
from smartdialer.providers.mock import PROFILE_A, PROFILE_B, MockProvider
from smartdialer.repo.campaigns import create_campaign
from smartdialer.repo.models import Mode
from smartdialer.scheduler import Scheduler
from smartdialer.sim.dbutil import ensure_sim_database
from smartdialer.sim.scenarios import SCENARIOS, Scenario

STEADY_AFTER_S = 180  # "steady state" metrics ignore the first 3 minutes (predictive is learning)
SERIES_EVERY_S = 10


@dataclass(frozen=True)
class RunSpec:
    scenario: str
    provider: str  # "A" | "B"
    mode: str  # "PROGRESSIVE" | "PREDICTIVE"
    seed: int = 1
    agents: int | None = None  # override the scenario's
    duration: int | None = None
    db_name: str = "dialer_sim"
    epsilon: float | None = None  # STRESS: weaken the controller's main rule to exercise its backstops


@dataclass
class _Recorder:
    online_s: int = 0
    connected_s: int = 0
    waiting_s: int = 0
    wrap_s: int = 0
    steady_online_s: int = 0
    steady_connected_s: int = 0
    ringing_sum: int = 0
    ringing_max: int = 0
    proposed_sum: int = 0
    approved_sum: int = 0
    ticks: int = 0
    fallback_reasons: dict = field(default_factory=lambda: {"circuit": 0, "shock": 0, "abandon_budget": 0})
    peak_rolling_abandon_rate: float = 0.0
    first_predictive_s: int | None = None
    series: list = field(default_factory=list)
    reconcile: dict = field(default_factory=lambda: dataclasses.asdict(ReconcileReport()))
    reconcile_passes: int = 0


async def _bulk_seed(pool, campaign_id: int, agents: int, borrowers: int, now) -> list[int]:
    async with pool.acquire() as conn:
        ids = await conn.fetch(
            "INSERT INTO agents (campaign_id, name, state, state_since) "
            "SELECT $1, 'agent' || g, 'AVAILABLE', $2 FROM generate_series(1, $3) g RETURNING id",
            campaign_id, now, agents)
        await conn.execute(
            "INSERT INTO borrowers (campaign_id, phone, next_attempt_at) "
            "SELECT $1, '+1555' || lpad(g::text, 7, '0'), $2 FROM generate_series(1, $3) g",
            campaign_id, now, borrowers)
    return sorted(r["id"] for r in ids)


async def run_scenario(spec: RunSpec) -> dict:
    sc: Scenario = SCENARIOS[spec.scenario]
    agents = spec.agents or sc.agents
    duration = spec.duration or sc.duration
    mode = Mode(spec.mode)
    wall_start = time.perf_counter()

    dsn = await ensure_sim_database(spec.db_name)
    admin = await asyncpg.connect(dsn)
    try:
        await db.reset_schema(admin)  # guarded: only *_sim / *_test databases
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=24)
    try:
        return await _run(pool, spec, sc, agents, duration, mode, wall_start)
    finally:
        await pool.close()


async def _run(pool, spec: RunSpec, sc: Scenario, agents: int, duration: int, mode: Mode, wall_start: float) -> dict:
    sched = Scheduler(VirtualClock())
    t0 = sched.now()
    cfg = DialerConfig()
    behavior = CallBehavior(answer_rate=sc.answer_rate(0), mean_talk_seconds=sc.mean_talk(0))
    profile = PROFILE_A if spec.provider == "A" else PROFILE_B
    raw = MockProvider(spec.provider, profile, behavior, sched, random.Random(spec.seed))
    provider = ProtectedProvider(raw, CircuitBreaker(spec.provider, sched.clock))

    async with pool.acquire() as conn:
        camp = await create_campaign(conn, sc.key, mode, sched.now(), wrap_up_seconds=sc.wrap_up)
    # Plenty of borrowers: the run must never be limited by an empty list.
    agent_ids = await _bulk_seed(pool, camp.id, agents, max(2000, agents * duration // 3), sched.now())

    scfg = SafetyConfig() if spec.epsilon is None else SafetyConfig(epsilon=spec.epsilon, epsilon_hangup=min(1.0, spec.epsilon * 5))
    safety = SafetyController(pool, provider.health, permit_ttl_seconds=cfg.permit_ttl_seconds, cfg=scfg)
    pacer = PredictivePacer() if mode is Mode.PREDICTIVE else ProgressivePacer()
    worker = DialerWorker(pool=pool, sched=sched, campaign_id=camp.id, provider=provider, pacer=pacer,
                          safety=safety, cfg=cfg)
    raw.set_event_handler(worker.events.handle)
    reconciler = Reconciler(pool, sched, provider, cfg, processor=worker.events)
    rec = _Recorder()
    online = list(agent_ids)
    dropped: list[list[int]] = []

    async def reconcile_pass():
        rep = await reconciler.run_once(camp.id)
        rec.reconcile_passes += 1
        for k, v in dataclasses.asdict(rep).items():
            rec.reconcile[k] += int(v)

    for t in range(duration):
        # ---- the world changes --------------------------------------------------------
        behavior.answer_rate, behavior.mean_talk_seconds = sc.answer_rate(t), sc.mean_talk(t)
        for start, end in sc.outages:
            if t == start:
                raw.start_outage()
            if t == end:
                raw.end_outage()
        for i, drop in enumerate(sc.agent_drops):
            if t == drop.at:
                n = int(len(online) * drop.fraction)
                gone, online = online[:n], online[n:]
                for a in gone:
                    await worker.agents.agent_offline(a)
                dropped.append(gone)
            if drop.restore_at is not None and t == drop.restore_at and i < len(dropped):
                for a in dropped[i]:
                    await worker.agents.agent_login(a)
                online = sorted(online + dropped[i])

        # ---- the dialer works ---------------------------------------------------------
        report = await worker.tick()
        _record_tick(rec, report, t, sc)
        async with pool.acquire() as conn:
            counts = {r["state"]: r["n"] for r in await conn.fetch(
                "SELECT state, count(*) AS n FROM agents WHERE campaign_id = $1 GROUP BY state", camp.id)}
        _record_agents(rec, counts, t)
        if t % SERIES_EVERY_S == 0:
            rec.series.append(_series_point(t, counts, report, sc, sched))
        if t % int(cfg.reconcile_interval_seconds) == 0:
            sched.spawn(reconcile_pass())
        await sched.advance(1)

    return await _summarise(pool, spec, sc, agents, duration, camp.id, worker, raw, provider, rec,
                            time.perf_counter() - wall_start, t0)


# ---- recording --------------------------------------------------------------------------

def _record_tick(rec: _Recorder, report, t: int, sc: Scenario) -> None:
    rec.ticks += 1
    rec.proposed_sum += report.proposal.n
    d = report.decision
    if d is None:
        return
    rec.approved_sum += d.approved_n
    inputs = d.inputs
    rec.ringing_sum += inputs.get("agentless_live_calls", 0)
    rec.ringing_max = max(rec.ringing_max, inputs.get("agentless_live_calls", 0))
    if d.verdict.value == "FALLBACK_PROGRESSIVE":
        for key, needle in (("circuit", "circuit"), ("shock", "shock"), ("abandon_budget", "abandon budget")):
            rec.fallback_reasons[key] += needle in d.reason
    if d.permit is not None and d.permit.kind is PermitKind.PREDICTIVE and rec.first_predictive_s is None:
        rec.first_predictive_s = t
    if inputs.get("answered_window", 0) >= 30:
        rec.peak_rolling_abandon_rate = max(rec.peak_rolling_abandon_rate,
                                            inputs["abandoned_window"] / inputs["answered_window"])


def _record_agents(rec: _Recorder, counts: dict, t: int) -> None:
    online = sum(n for s, n in counts.items() if s not in ("OFFLINE", "PAUSED"))
    connected = counts.get("CONNECTED", 0)
    rec.online_s += online
    rec.connected_s += connected
    rec.waiting_s += counts.get("AVAILABLE", 0) + counts.get("RESERVED", 0) + counts.get("DIALING", 0)
    rec.wrap_s += counts.get("WRAP_UP", 0)
    if t >= STEADY_AFTER_S:
        rec.steady_online_s += online
        rec.steady_connected_s += connected


def _series_point(t: int, counts: dict, report, sc: Scenario, sched) -> dict:
    d = report.decision
    online = sum(n for s, n in counts.items() if s not in ("OFFLINE", "PAUSED"))
    return {
        "t": t, "online": online, "connected": counts.get("CONNECTED", 0),
        "waiting": counts.get("AVAILABLE", 0) + counts.get("RESERVED", 0) + counts.get("DIALING", 0),
        "wrap_up": counts.get("WRAP_UP", 0),
        "ringing_agentless": d.inputs.get("agentless_live_calls", 0) if d else None,
        "proposed": report.proposal.n, "approved": d.approved_n if d else 0,
        "verdict": d.verdict.value if d else None,
        "est_answer_rate": report.proposal.details.get("answer_rate"),
        "pessimistic_answer_rate": d.inputs.get("q_pessimistic") if d else None,
        "true_answer_rate": sc.answer_rate(t),
    }


async def _summarise(pool, spec, sc, agents, duration, cid, worker, raw, provider, rec: _Recorder, wall_s, t0) -> dict:
    async with pool.acquire() as conn:
        c = await conn.fetchrow(
            "SELECT count(*) FILTER (WHERE initiated_at IS NOT NULL) AS initiated, "
            "count(*) FILTER (WHERE answered_at IS NOT NULL) AS answered, "
            "count(*) FILTER (WHERE connected_at IS NOT NULL) AS connected, "
            "count(*) FILTER (WHERE state = 'COMPLETED') AS completed, "
            "count(*) FILTER (WHERE end_reason = 'ABANDONED') AS abandoned, "
            "count(*) FILTER (WHERE end_reason = 'HUNG_UP_EXCESS') AS hung_up_excess "
            "FROM calls WHERE campaign_id = $1", cid)
        # Calls WE cancelled while they looked like they were ringing, for which the provider later reported
        # ANSWERED: the borrower had picked up before our hangup (their callback was still in flight).
        hung_answered = await conn.fetchval(
            "SELECT count(DISTINCT c.id) FROM calls c JOIN provider_events e ON e.call_id = c.id "
            "WHERE c.campaign_id = $1 AND c.end_reason = 'HUNG_UP_EXCESS' AND e.event_type = 'ANSWERED'", cid)
        reasons = {(r["end_reason"] or "-"): r["n"] for r in await conn.fetch(
            "SELECT end_reason, count(*) AS n FROM calls WHERE campaign_id = $1 AND state IN ('FAILED','CANCELLED') "
            "GROUP BY end_reason ORDER BY n DESC", cid)}
        verdicts = {r["verdict"]: r["n"] for r in await conn.fetch(
            "SELECT verdict, count(*) AS n FROM safety_decisions WHERE campaign_id = $1 GROUP BY verdict", cid)}
        borrowers = {r["status"]: r["n"] for r in await conn.fetch(
            "SELECT status, count(*) AS n FROM borrowers WHERE campaign_id = $1 GROUP BY status", cid)}
    out = dict(c)
    out["hung_up_but_answered"] = hung_answered
    # Abandons for compliance = answered calls with no agent (ABANDONED) PLUS answered calls we hung up on
    # (dead air, the provider's ANSWERED callback was still in flight). Both count as answered.
    out["abandoned_recorded"] = out["abandoned"]
    out["abandoned"] = out["abandoned"] + hung_answered
    out["answered_effective"] = out["answered"] + hung_answered
    ticks = max(1, rec.ticks)
    return {
        "scenario": sc.key, "scenario_name": sc.name, "provider": spec.provider, "mode": spec.mode,
        "seed": spec.seed, "epsilon": spec.epsilon, "agents": agents, "duration_s": duration, "wall_seconds": round(wall_s, 1),
        "utilization": {
            "connected": rec.connected_s / max(1, rec.online_s),
            "waiting": rec.waiting_s / max(1, rec.online_s),
            "wrap_up": rec.wrap_s / max(1, rec.online_s),
            "connected_steady": rec.steady_connected_s / max(1, rec.steady_online_s),
        },
        "calls": {**out, "abandon_rate": out["abandoned"] / max(1, out["answered_effective"]),
                  "ended_without_answer_or_completion": reasons},
        "pacing": {"avg_proposed": rec.proposed_sum / ticks, "avg_approved": rec.approved_sum / ticks,
                   "avg_ringing_agentless": rec.ringing_sum / ticks, "max_ringing_agentless": rec.ringing_max,
                   "first_predictive_second": rec.first_predictive_s},
        "safety": {"decisions": verdicts,
                   "fallback_reasons": rec.fallback_reasons,
                   "peak_rolling_abandon_rate": rec.peak_rolling_abandon_rate},
        "provider_incidents": {"stats": dataclasses.asdict(raw.stats),
                     "breaker_transitions": [[round((ts - t0).total_seconds()), st.value] for ts, st in provider.breaker.transitions],
                     "reconciler": {**rec.reconcile, "passes": rec.reconcile_passes}},
        "dialer": {"abandons_counted": worker.stats.abandons, "late_bound": worker.stats.late_bound,
                   "hung_up_excess": worker.stats.hung_up_excess, "compensating_hangups": worker.stats.compensating_hangups},
        "borrowers": borrowers,
        "series": rec.series,
    }
