"""Safety Controller: the only component that can authorise a call.

    Pacer --Proposal--> SafetyController --Permit--> CallAllocator --> Provider

Why the allocator cannot bypass this
------------------------------------
* The allocator's only entry point takes a Permit, and the controller will only honour a
  Permit it issued itself: it keeps a private registry of live permits and the allocator must
  call `redeem()` once per call it wants to start. A Permit built by hand, copied from another
  controller, expired, already spent or for another campaign is refused. There is no flag, no
  config value and no "unsafe" mode: the only way to get a Permit is `review()`.
* Permit's constructor demands a module-private token, so ordinary code cannot even build one.
  (Python cannot make that absolute, which is exactly why the registry exists: forging the
  object still gets you nothing at redeem time.)
* The controller never trusts the pacer's numbers. `review()` receives only (n, reason) and
  re-reads live counts and outcome statistics from PostgreSQL itself. It also decides the
  campaign's mode from the database, not from what the proposal claims, so a pacer cannot opt
  itself into predictive dialing.
* Permits live in this process's memory on purpose: a permit is a short-lived (seconds)
  authorisation, never persisted, never sent to another worker. A crash simply voids it.

What it decides
---------------
PROGRESSIVE campaigns: approve at most one call per agent that is free right now, cross-checked
against the calls table (two independent counts; disagreement can only mean "dial less").

PREDICTIVE campaigns, in this order:
  1. campaign not ACTIVE, or provider circuit open and cooling down      -> REJECT
  2. FALLBACK_PROGRESSIVE (behave exactly like progressive) if ANY of:
       * circuit breaker not CLOSED            (the provider is unhealthy)
       * answer-rate shock                      (recent rate differs from the rate before it)
       * rolling abandon budget near its limit  (one more abandon would use >= 80% of it; with no
                                                 data yet this is true, so a new campaign starts
                                                 progressive and earns predictive dialing)
  3. otherwise size by statistics:
       q  = pessimistic answer rate = max(Wilson upper bound over the long window,
                                          Wilson upper bound over the short window)
       F  = agents free now + agents whose wrap-up ends before even the FASTEST calls (10th
            percentile of observed setup time) could be answered
       m  = the largest number of outstanding calls with P(Binomial(m, q) > F) <= 1%
       allowed new calls = min(proposed, m - calls already ringing)
  4. Independently of dialing, if far more calls are ringing than the current F allows (agents
     just dropped, the rate surged), order the excess hung up. Not an abandon: nobody answered.

Every verdict is persisted with the numbers behind it (safety_decisions).
"""

from __future__ import annotations

import enum
import itertools
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from smartdialer.dialer.config import SafetyConfig
from smartdialer.dialer.pacing import Proposal
from smartdialer.dialer.safetymath import max_safe_outstanding, rate_shock, wilson_upper
from smartdialer.providers.breaker import BreakerState, ProviderHealth
from smartdialer.repo.campaigns import get_campaign
from smartdialer.repo.models import Mode
from smartdialer.repo.safety_log import log_decision
from smartdialer.repo.snapshot import CampaignSnapshot, load_snapshot
from smartdialer.repo.stats import RateStats, load_rate_stats

_MINT = object()  # only this module can present it


class Verdict(enum.StrEnum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_PROGRESSIVE = "FALLBACK_PROGRESSIVE"


class PermitKind(enum.StrEnum):
    PROGRESSIVE = "PROGRESSIVE"  # each call must reserve an agent BEFORE dialing
    PREDICTIVE = "PREDICTIVE"  # calls dial without an agent; one is taken atomically on answer


@dataclass(frozen=True, slots=True)
class Permit:
    """Authorisation to start up to `n` calls for one campaign, until `expires_at`."""
    id: int
    campaign_id: int
    n: int
    kind: PermitKind
    expires_at: datetime
    _token: object = field(repr=False, compare=False, default=None)

    def __post_init__(self):
        if self._token is not _MINT:
            raise PermissionError("Permits can only be issued by SafetyController.review()")


@dataclass(frozen=True, slots=True)
class Decision:
    verdict: Verdict
    proposed_n: int
    approved_n: int
    reason: str
    permit: Permit | None
    inputs: dict
    hangup_n: int = 0  # still-ringing agentless calls the controller orders hung up


@dataclass(slots=True)
class _Outcome:
    verdict: Verdict
    approved: int
    reason: str
    kind: PermitKind = PermitKind.PROGRESSIVE
    hangup_n: int = 0


class SafetyController:
    def __init__(self, pool, health_source: Callable[[], ProviderHealth] | None = None, *,
                 permit_ttl_seconds: float = 5.0, cfg: SafetyConfig | None = None):
        self._pool = pool
        self._health = health_source
        self._ttl = timedelta(seconds=permit_ttl_seconds)
        self.cfg = cfg or SafetyConfig()
        self._ids = itertools.count(1)
        self._live: dict[int, list] = {}  # permit id -> [the issued Permit object, calls remaining]

    # ---- review ----------------------------------------------------------------------
    async def review(self, proposal: Proposal, campaign_id: int, now: datetime) -> Decision:
        """Decide how much of `proposal` is allowed, mint a Permit for it, and log why."""
        async with self._pool.acquire() as conn:
            snap = await load_snapshot(conn, campaign_id, now)  # fresh: ignores the pacer's view
            campaign = await get_campaign(conn, campaign_id)
            stats = None
            if snap.mode is Mode.PREDICTIVE:
                c = self.cfg
                stats = await load_rate_stats(conn, campaign_id, now, short_s=c.short_window_seconds,
                                              long_s=c.long_window_seconds, abandon_s=c.abandon_window_seconds)
            inputs = {
                "proposal_reason": proposal.reason,
                "campaign_mode": snap.mode,
                "campaign_status": snap.status,
                "available_agents": snap.available_agents,
                "agents_online": snap.agents_online,
                "agent_bound_live_calls": snap.agent_bound_live_calls,
                "agentless_live_calls": snap.agentless_live_calls,
                "dialable_borrowers": snap.dialable_borrowers,
            }
            out = self._decide(proposal, snap, campaign, stats, now, inputs)
            permit = self._mint(campaign_id, out.approved, out.kind, now) if out.approved > 0 else None
            inputs["hangup_n"] = out.hangup_n
            decision = Decision(out.verdict, proposal.n, out.approved, out.reason, permit, inputs, out.hangup_n)
            if proposal.n > 0 or out.hangup_n > 0:  # idle ticks (nothing to decide) would only add noise
                await log_decision(conn, campaign_id=campaign_id, now=now, proposed_n=proposal.n,
                                   approved_n=out.approved, verdict=out.verdict.value, reason=out.reason,
                                   inputs=inputs)
        return decision

    # ---- policy ----------------------------------------------------------------------
    def _decide(self, proposal: Proposal, snap: CampaignSnapshot, campaign, stats: RateStats | None,
                now: datetime, inputs: dict) -> _Outcome:
        if snap.status != "ACTIVE":
            return _Outcome(Verdict.REJECT, 0, f"campaign is {snap.status}")

        health_cap, health_note = self._health_cap(now, inputs)
        if health_cap == 0:
            return _Outcome(Verdict.REJECT, 0, health_note)

        if snap.mode is Mode.PROGRESSIVE:
            if proposal.n <= 0:
                return _Outcome(Verdict.REJECT, 0, "nothing proposed")
            approved, caps = self._progressive_n(proposal.n, snap, health_cap, inputs)
            if approved == 0:
                return _Outcome(Verdict.REJECT, 0, f"no agent can take a call ({caps}){health_note}")
            if approved < proposal.n:
                return _Outcome(Verdict.REDUCE, approved, f"reduced {proposal.n} -> {approved}: {caps}{health_note}")
            return _Outcome(Verdict.APPROVE, approved, f"approved {approved}: one call per available agent{health_note}")

        return self._decide_predictive(proposal, snap, campaign, stats, health_cap, health_note, inputs)

    def _health_cap(self, now: datetime, inputs: dict) -> tuple[int | None, str]:
        """(cap, note). cap None = unrestricted; 0 = reject; 1 = single probe call."""
        if self._health is None:
            return None, ""
        h = self._health()
        inputs["provider_state"] = h.state
        inputs["provider_failure_ratio"] = round(h.failure_ratio, 3)
        if h.state is BreakerState.OPEN and h.retry_at is not None and now < h.retry_at:
            return 0, f"provider circuit open until {h.retry_at.isoformat()}"
        if h.state is not BreakerState.CLOSED:
            return 1, f"; provider {h.state}: single probe call only"
        return None, ""

    def _progressive_n(self, n: int, snap: CampaignSnapshot, health_cap: int | None, inputs: dict) -> tuple[int, str]:
        """Progressive sizing: two INDEPENDENT counts must both allow the call:
          (a) agents sitting AVAILABLE right now, minus agentless calls already ringing (each will
              want one of them if answered: this is what keeps a fallback from stealing the agents
              that predictive calls in flight are counting on);
          (b) online agents minus agents already tied to a live call (from the calls table).
        They agree unless something is wrong; taking the minimum means a bookkeeping bug can only
        make us dial LESS."""
        by_available = max(0, snap.available_agents - snap.agentless_live_calls)
        by_calls = max(0, snap.agents_online - snap.agent_bound_live_calls - snap.agentless_live_calls)
        inputs["cap_by_available_agents"], inputs["cap_by_live_calls"] = by_available, by_calls
        approved = max(0, min(n, by_available, by_calls, health_cap if health_cap is not None else n))
        return approved, f"available agents={snap.available_agents}, ringing agentless={snap.agentless_live_calls}, " \
                         f"online-minus-busy={by_calls}"

    def _decide_predictive(self, proposal, snap, campaign, stats: RateStats, health_cap, health_note, inputs) -> _Outcome:
        c = self.cfg
        # ---- the evidence, all measured here, none of it taken from the pacer ---------------
        horizon = min(c.max_credit_horizon_seconds,
                      max(c.min_credit_horizon_seconds, stats.setup_p10 or c.default_credit_horizon_seconds))
        wrap_soon = snap.wrap_ups_ending_within(horizon)
        free = snap.available_agents + wrap_soon
        q_long = wilson_upper(stats.k_long, stats.n_long, c.z)
        q_short = wilson_upper(stats.k_short, stats.n_short, c.z)
        q = max(q_long, q_short)  # a recent surge shows up in the short window before the long one
        m_max = max_safe_outstanding(free, q, c.epsilon, c.max_ratio)  # stop DIALING above this
        m_hang = max_safe_outstanding(free, q, c.epsilon_hangup, c.max_ratio)  # only HANG UP above this
        ringing_all = snap.agentless_live_calls
        ringing = snap.young_agentless()  # only calls that can still be answered enter the overflow maths
        abandoned, answered = stats.abandoned_in_abandon_window, stats.answered_in_abandon_window
        budget = campaign.max_abandon_rate
        shock, shock_info = rate_shock(stats.k_short, stats.n_short, stats.k_long - stats.k_short,
                                       stats.n_long - stats.n_short, min_samples=c.shock_min_samples,
                                       min_gap=c.shock_min_gap, sigmas=c.shock_sigmas)
        next_rate = (abandoned + 1) / max(answered, 1)
        inputs.update({
            "free_agents": free, "wrap_ups_credited": wrap_soon, "credit_horizon_s": round(horizon, 1),
            "resolved_long": stats.n_long, "answered_long": stats.k_long,
            "resolved_short": stats.n_short, "answered_short": stats.k_short,
            "q_upper_long": round(q_long, 4), "q_upper_short": round(q_short, 4), "q_pessimistic": round(q, 4),
            "max_safe_outstanding": m_max, "max_outstanding_before_hangup": m_hang, "ringing_agentless": ringing_all, "ringing_young": ringing,
            "answer_age_p99": snap.predictive.answer_age_p99 if snap.predictive else None,
            "abandoned_window": abandoned, "answered_window": answered, "abandon_budget": budget,
            "abandon_rate_after_one_more": round(next_rate, 4), "shock": shock_info,
        })

        fallback_reasons = []
        if health_cap is not None:
            fallback_reasons.append(f"provider circuit not closed{health_note}")
        if shock:
            fallback_reasons.append(f"answer-rate shock ({shock_info['p_before']:.0%} -> {shock_info['p_recent']:.0%}, "
                                    f"threshold {shock_info['threshold']:.0%})")
        if next_rate > c.abandon_near_fraction * budget:
            fallback_reasons.append(
                f"abandon budget near limit ({abandoned} abandoned of {answered} answered in {c.abandon_window_seconds:.0f}s; "
                f"one more = {next_rate:.1%} > {c.abandon_near_fraction:.0%} of the {budget:.0%} limit)")

        if fallback_reasons:
            # Deterministic mode: only agents provably free, and none already spoken for by ringing calls.
            approved, caps = self._progressive_n(proposal.n, snap, health_cap, inputs)
            hangup = self._excess(ringing_all, snap.hangable_calls, free)  # cover every ringing call with an agent
            return _Outcome(Verdict.FALLBACK_PROGRESSIVE, approved,
                            f"fallback to progressive: {'; '.join(fallback_reasons)}. approved {approved} of {proposal.n} ({caps})"
                            + (f"; hanging up {hangup} excess ringing" if hangup else ""),
                            PermitKind.PROGRESSIVE, hangup)

        hangup = min(max(0, ringing - m_hang), snap.hangable_calls)
        room = max(0, m_max - ringing)
        approved = min(proposal.n, room)
        basis = (f"free agents={free} ({snap.available_agents} available + {wrap_soon} wrap-ups ending within "
                 f"{horizon:.0f}s), pessimistic answer rate={q:.0%} (Wilson upper; long {q_long:.0%}, short {q_short:.0%}), "
                 f"max outstanding with P(answers>free)<={c.epsilon:.0%} = {m_max} (hang up only above {m_hang}, risk>{c.epsilon_hangup:.0%}), "
                 f"ringing and still answerable={ringing} (of {ringing_all} ringing)")
        if hangup:
            return _Outcome(Verdict.REJECT, 0, f"too many calls ringing for the agents free now: hanging up {hangup}. {basis}",
                            PermitKind.PREDICTIVE, hangup)
        if approved == 0:
            return _Outcome(Verdict.REJECT, 0, f"safe outstanding limit reached: {basis}", PermitKind.PREDICTIVE)
        if approved < proposal.n:
            return _Outcome(Verdict.REDUCE, approved, f"reduced {proposal.n} -> {approved}: {basis}", PermitKind.PREDICTIVE)
        return _Outcome(Verdict.APPROVE, approved, f"approved {approved}: {basis}", PermitKind.PREDICTIVE)

    def _excess(self, ringing: int, hangable: int, limit: int) -> int:
        """FALLBACK mode only: how many ringing calls to hang up so that every one has an agent.
        Small overshoots are tolerated (count-based hysteresis). In normal predictive mode the
        hysteresis is in probability space instead (stop dialing at epsilon, hang up only above
        epsilon_hangup): a count tolerance misfires when the allowed outstanding is ~4x the free
        agents, because every answer then lowers the limit by ~4 while removing one ringing call."""
        excess = ringing - limit
        if excess <= max(2, math.ceil(0.2 * limit)):
            return 0
        return min(excess, hangable)

    # ---- permits ---------------------------------------------------------------------
    def _mint(self, campaign_id: int, n: int, kind: PermitKind, now: datetime) -> Permit:
        p = Permit(next(self._ids), campaign_id, n, kind, now + self._ttl, _MINT)
        self._live[p.id] = [p, n]
        return p

    def redeem(self, permit: Permit, now: datetime) -> bool:
        """Spend ONE call of the permit. False means: do not dial."""
        entry = self._live.get(permit.id)
        # Identity, not equality: a look-alike object carrying a live permit's id is refused.
        if entry is None or entry[0] is not permit:
            return False
        if entry[1] <= 0 or now > permit.expires_at:
            del self._live[permit.id]
            return False
        entry[1] -= 1
        return True

    def void(self, permit: Permit) -> None:
        """Hand back whatever is left (e.g. no borrower to dial)."""
        self._live.pop(permit.id, None)

    @property
    def outstanding_permits(self) -> int:
        return len(self._live)
