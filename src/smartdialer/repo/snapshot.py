"""One consistent read of everything a pacer or the Safety Controller needs to size a batch."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from smartdialer.repo.models import Mode
from smartdialer.repo.stats import load_answer_age_p99, load_avg_setup_seconds, load_recent_outcomes
from smartdialer.states import NON_TERMINAL_CALL_STATES, AgentState, CallState


@dataclass(frozen=True, slots=True)
class PredictiveInputs:
    """Extra numbers only predictive pacing needs (read only for PREDICTIVE campaigns)."""
    outcomes: tuple  # ((datetime, answered: bool), ...) recent resolved calls, newest first
    avg_setup_seconds: float | None  # mean time from dialing to answer; None = no data yet
    wrap_up_deadlines: tuple  # wrap_up_until of every agent in WRAP_UP
    # Longest time-to-answer actually observed (p99), None until there is enough data. A call that has
    # rung LONGER than this is almost certainly a no-answer waiting out its timeout.
    answer_age_p99: float | None = None
    agentless_ages: tuple = ()  # seconds since dialing, for every agentless call still ringing/queued


@dataclass(frozen=True, slots=True)
class CampaignSnapshot:
    campaign_id: int
    mode: Mode
    status: str
    now: datetime
    agents: Mapping[AgentState, int]
    calls: Mapping[CallState, int]  # non-terminal calls only
    agent_bound_live_calls: int  # live calls that hold an agent (agent_id IS NOT NULL)
    dialable_borrowers: int  # PENDING and due; capped, so "at least this many"
    agentless_live_calls: int = 0  # live calls with no agent yet: they will each need one on answer
    hangable_calls: int = 0  # ... of which still INITIATED/RINGING (can be hung up without harm)
    predictive: PredictiveInputs | None = None

    @property
    def available_agents(self) -> int:
        return self.agents[AgentState.AVAILABLE]

    @property
    def agents_online(self) -> int:
        """Agents able to take calls now or soon: everything except OFFLINE and PAUSED."""
        return sum(n for s, n in self.agents.items() if s not in (AgentState.OFFLINE, AgentState.PAUSED))

    def young_agentless(self) -> int:
        """Agentless calls that can still plausibly be ANSWERED: those that have rung no longer than the
        slowest answer we have measured (default 30 s, the ring timeout, until we have data). Older
        ones are counted by neither the pacer nor the Safety Controller's overflow maths: treating a
        call that has rung 28 s with the same answer probability as a fresh one makes the controller
        hang up healthy calls whenever answers consume agents."""
        pi = self.predictive
        if pi is None or not pi.agentless_ages:
            return self.agentless_live_calls
        limit = pi.answer_age_p99 if pi.answer_age_p99 is not None else 30.0
        return sum(1 for a in pi.agentless_ages if a <= limit)

    def wrap_ups_ending_within(self, seconds: float) -> int:
        """Agents in WRAP_UP who will be AVAILABLE within `seconds`."""
        if self.predictive is None:
            return 0
        return sum(1 for t in self.predictive.wrap_up_deadlines if (t - self.now).total_seconds() <= seconds)


async def load_snapshot(conn, campaign_id: int, now: datetime, *, borrower_cap: int = 1000,
                        outcome_window_s: float = 600.0) -> CampaignSnapshot:
    # REPEATABLE READ + read-only: all queries see the same instant, so the counts add up.
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        c = await conn.fetchrow("SELECT mode, status FROM campaigns WHERE id = $1", campaign_id)
        agent_rows = await conn.fetch(
            "SELECT state, count(*) AS n FROM agents WHERE campaign_id = $1 GROUP BY state", campaign_id)
        call_rows = await conn.fetch(
            "SELECT state, count(*) AS n, count(agent_id) AS with_agent FROM calls "
            "WHERE campaign_id = $1 AND state = ANY($2::text[]) GROUP BY state",
            campaign_id, [s.value for s in NON_TERMINAL_CALL_STATES])
        dialable = await conn.fetchval(
            "SELECT count(*) FROM (SELECT 1 FROM borrowers WHERE campaign_id = $1 AND status = 'PENDING' "
            "AND next_attempt_at <= $2 LIMIT $3) t", campaign_id, now, borrower_cap)
        mode = Mode(c["mode"])
        predictive = None
        if mode is Mode.PREDICTIVE:
            outcomes = await load_recent_outcomes(conn, campaign_id, now, outcome_window_s)
            setup = await load_avg_setup_seconds(conn, campaign_id, now, outcome_window_s)
            wraps = await conn.fetch(
                "SELECT wrap_up_until FROM agents WHERE campaign_id = $1 AND state = 'WRAP_UP' "
                "AND wrap_up_until IS NOT NULL LIMIT 20000", campaign_id)
            p99 = await load_answer_age_p99(conn, campaign_id, now, outcome_window_s)
            ages = await conn.fetch(
                "SELECT extract(epoch FROM ($2::timestamptz - coalesce(initiated_at, created_at))) AS age FROM calls "
                "WHERE campaign_id = $1 AND agent_id IS NULL AND state IN ('QUEUED', 'INITIATED', 'RINGING')", campaign_id, now)
            predictive = PredictiveInputs(tuple(outcomes), setup, tuple(r["wrap_up_until"] for r in wraps),
                                          p99, tuple(float(r["age"]) for r in ages))
    agents = {s: 0 for s in AgentState}
    agents.update({AgentState(r["state"]): r["n"] for r in agent_rows})
    calls = {CallState(r["state"]): r["n"] for r in call_rows}
    agentless = sum(r["n"] - r["with_agent"] for r in call_rows)
    hangable = sum(r["n"] - r["with_agent"] for r in call_rows
                   if r["state"] in (CallState.INITIATED.value, CallState.RINGING.value))
    return CampaignSnapshot(campaign_id, mode, c["status"], now, agents, calls,
                            sum(r["with_agent"] for r in call_rows), dialable,
                            agentless, hangable, predictive)
