"""Pacing engines. They answer ONE question, "how many calls would we like to start now?",
and can do nothing else.

The separation is structural, not a convention: a pacer is a pure function from a
CampaignSnapshot (numbers) to a Proposal (a number and a reason). It holds no database
connection, no provider, no allocator and no reference to the Safety Controller, so there is
no code path by which it could place a call or switch safety off.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from smartdialer.dialer.config import PacerConfig
from smartdialer.dialer.safetymath import decayed_answer_rate
from smartdialer.repo.models import Mode
from smartdialer.repo.snapshot import CampaignSnapshot


@dataclass(frozen=True, slots=True)
class Proposal:
    n: int
    reason: str  # human-readable: "why this many, right now"
    mode: Mode
    details: dict = field(default_factory=dict)  # the numbers behind the reason


class Pacer(Protocol):
    def propose(self, snap: CampaignSnapshot) -> Proposal: ...


class ProgressivePacer:
    """One available agent -> one call. The simplest possible rule, and the safe one: the
    number of calls can never exceed the number of agents we already hold."""

    def propose(self, snap: CampaignSnapshot) -> Proposal:
        avail, dialable = snap.available_agents, snap.dialable_borrowers
        n = max(0, min(avail, dialable))
        if avail == 0:
            why = "no available agents"
        elif dialable == 0:
            why = f"{avail} available agents but no dialable borrowers"
        else:
            why = f"progressive: 1 call per available agent ({avail} available, {dialable}+ dialable borrowers)"
        return Proposal(n, why, Mode.PROGRESSIVE, {"available_agents": avail, "dialable_borrowers": dialable})


class PredictivePacer:
    """Dial ahead of agent availability. The (optimistic) estimate of how many calls to have
    ringing so that, on average, one answer arrives per free agent:

        free     = AVAILABLE agents + agents whose wrap-up ends within one call-setup time
        p        = time-decayed (EWMA) answer rate, floored
        target   = floor(free / p)            calls we would like outstanding (ringing)
        propose  = target - already ringing   (never negative)

    Example: 10 free agents, p = 0.5 -> target 20; 12 already ringing -> propose 8.

    This is a *proposal*. The pacer is optimistic by design (a point estimate, no safety margin);
    the Safety Controller, which uses a pessimistic bound and its own fresh data, decides how much
    of it is allowed. The reasoning travels with the proposal so any decision can be explained."""

    def __init__(self, cfg: PacerConfig | None = None):
        self.cfg = cfg or PacerConfig()

    def propose(self, snap: CampaignSnapshot) -> Proposal:
        cfg, pi = self.cfg, snap.predictive
        outcomes = pi.outcomes if pi else ()
        setup = (pi.avg_setup_seconds if pi else None) or cfg.default_setup_seconds
        p_raw, n_eff = decayed_answer_rate(outcomes, snap.now, cfg.half_life_seconds, cfg.prior_rate, cfg.prior_weight)
        p = max(cfg.min_rate, p_raw)
        wrap_soon = snap.wrap_ups_ending_within(setup)
        free = snap.available_agents + wrap_soon
        ringing = snap.young_agentless()  # calls that can still be answered; stale rings don't need agents
        target = min(int(free / p), int(free * cfg.max_ratio))
        n = max(0, min(target - ringing, snap.dialable_borrowers))
        details = {"available_agents": snap.available_agents, "wrap_ups_soon": wrap_soon, "free": free,
                   "answer_rate": round(p, 4), "effective_samples": round(n_eff, 1),
                   "setup_seconds": round(setup, 1), "target_outstanding": target,
                   "already_ringing": ringing, "dialable_borrowers": snap.dialable_borrowers}
        if free == 0:
            why = f"no free agents ({wrap_soon} wrap-ups ending within {setup:.0f}s): nothing to pace for"
        else:
            why = (f"predictive: {free} free agents ({snap.available_agents} available + {wrap_soon} finishing wrap-up "
                   f"within the {setup:.0f}s setup time) at est. answer rate {p:.0%} -> target {target} outstanding "
                   f"- {ringing} already ringing = {max(0, target - ringing)}"
                   + (f", limited to {n} by dialable borrowers" if n < max(0, target - ringing) else ""))
        return Proposal(n, why, Mode.PREDICTIVE, details)
