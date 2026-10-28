"""Outcome statistics read straight from the calls table (the single source of truth).

ANSWER-RATE STATISTICS ARE MEASURED BY COHORT, NOT BY RESOLUTION TIME.
A call that will be answered resolves after ~6-24 s; one that will not rings out its 30 s timeout (plus callback
delay). If we counted outcomes as they resolve, right after a burst of dialing the recent window would contain
the fast answers and none of the slow no-answers: a transient, artificial surge in the "answer rate" that the
shock detector would (and in simulation, did) mistake for a real change. So a call enters the statistics by WHEN
IT WAS DIALED, and only once the whole cohort has had time to resolve (MATURITY_S after dialing): fast and slow
outcomes then count equally. The price is a lag of MATURITY_S in noticing a real change; the abandon budget and
the answerable-rings logic are the fast paths.

"Resolved" calls are those that tell us something about whether people pick up: answered ones
(answered_at set, including abandons) and unanswered ones (NO_ANSWER / BUSY). Calls ended by us
or by faults (CANCELLED, HUNG_UP_EXCESS, PROVIDER_ERROR, AGENT_LOST...) say nothing about
answer behaviour and are excluded.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

RESOLVED = "(answered_at IS NOT NULL OR end_reason IN ('NO_ANSWER', 'BUSY'))"
MATURITY_S = 35.0  # ring timeout (30 s) + margin: after this a dialed call's outcome is known


@dataclass(frozen=True, slots=True)
class RateStats:
    n_long: int  # resolved calls in the long window
    k_long: int  # ... of which answered
    n_short: int
    k_short: int
    answered_in_abandon_window: int
    abandoned_in_abandon_window: int
    setup_p10: float | None  # 10th percentile of INITIATED -> ANSWERED seconds (fast calls)


async def load_rate_stats(conn, campaign_id: int, now: datetime, *, short_s: float, long_s: float,
                          abandon_s: float) -> RateStats:
    mature = now - timedelta(seconds=MATURITY_S)  # only calls dialed before this have a settled outcome
    long_start, short_start = mature - timedelta(seconds=long_s), mature - timedelta(seconds=short_s)
    r = await conn.fetchrow(
        f"""
        WITH o AS (SELECT initiated_at AS t, (answered_at IS NOT NULL) AS ans
                     FROM calls WHERE campaign_id = $1 AND {RESOLVED}
                      AND initiated_at >= $2 AND initiated_at <= $4)
        SELECT count(*) AS n_long, count(*) FILTER (WHERE ans) AS k_long,
               count(*) FILTER (WHERE t >= $3) AS n_short,
               count(*) FILTER (WHERE ans AND t >= $3) AS k_short FROM o
        """, campaign_id, long_start, short_start, mature)
    a = await conn.fetchrow(
        "SELECT count(*) AS answered, count(*) FILTER (WHERE end_reason = 'ABANDONED') AS abandoned "
        "FROM calls WHERE campaign_id = $1 AND answered_at >= $2", campaign_id, now - timedelta(seconds=abandon_s))
    # DEAD AIR: calls WE cancelled as excess whose borrower had in fact already answered (the provider's
    # ANSWERED callback was still in flight). answered_at stays NULL on those, so they would vanish from
    # the abandon accounting. They are abandons in every sense that matters: count them, as answered too.
    dead_air = await conn.fetchval(
        "SELECT count(DISTINCT c.id) FROM calls c JOIN provider_events e ON e.call_id = c.id "
        "WHERE c.campaign_id = $1 AND c.end_reason = 'HUNG_UP_EXCESS' AND c.ended_at >= $2 AND e.event_type = 'ANSWERED'",
        campaign_id, now - timedelta(seconds=abandon_s))
    p10 = await conn.fetchval(
        "SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY extract(epoch FROM answered_at - initiated_at)) "
        "FROM calls WHERE campaign_id = $1 AND answered_at >= $2 AND initiated_at IS NOT NULL", campaign_id, long_start)
    return RateStats(r["n_long"], r["k_long"], r["n_short"], r["k_short"], a["answered"] + dead_air, a["abandoned"] + dead_air,
                     float(p10) if p10 is not None else None)


async def load_recent_outcomes(conn, campaign_id: int, now: datetime, window_s: float,
                               limit: int = 2000) -> list[tuple[datetime, bool]]:
    """(dialed_at, answered) of settled calls dialed in the window, newest first. See the header: by dial
    time and only once mature, so the pacer's moving average is not inflated by fast answers."""
    rows = await conn.fetch(
        f"SELECT initiated_at AS t, (answered_at IS NOT NULL) AS ans FROM calls "
        f"WHERE campaign_id = $1 AND {RESOLVED} AND initiated_at >= $2 AND initiated_at <= $3 "
        f"ORDER BY t DESC LIMIT $4", campaign_id, now - timedelta(seconds=window_s + MATURITY_S),
        now - timedelta(seconds=MATURITY_S), limit)
    return [(r["t"], r["ans"]) for r in rows]


async def load_avg_setup_seconds(conn, campaign_id: int, now: datetime, window_s: float) -> float | None:
    v = await conn.fetchval(
        "SELECT avg(extract(epoch FROM answered_at - initiated_at)) FROM calls "
        "WHERE campaign_id = $1 AND answered_at >= $2 AND initiated_at IS NOT NULL",
        campaign_id, now - timedelta(seconds=window_s))
    return float(v) if v is not None else None


async def load_answer_age_p99(conn, campaign_id: int, now: datetime, window_s: float,
                              min_samples: int = 50) -> float | None:
    """99th percentile of how long (INITIATED -> ANSWERED) answered calls took, or None when there are
    fewer than `min_samples` of them (a percentile of a handful of calls is noise, and a too-small value
    would make the controller discount calls that may still be answered). Clamped to [5, 60] seconds."""
    r = await conn.fetchrow(
        "SELECT count(*) AS n, percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM answered_at - initiated_at)) AS p99 "
        "FROM calls WHERE campaign_id = $1 AND answered_at >= $2 AND initiated_at IS NOT NULL",
        campaign_id, now - timedelta(seconds=window_s))
    if r["n"] < min_samples or r["p99"] is None:
        return None
    return min(60.0, max(5.0, float(r["p99"])))
