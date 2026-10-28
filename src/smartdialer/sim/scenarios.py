"""The workloads. A scenario says how borrowers behave over time (answer rate, talk time) and what
goes wrong (provider outages, agents vanishing). It contains no dialer logic.

A, B, C are the brief's steady cases. D changes while running, so the pacer has to follow the
world and the Safety Controller has to react to shocks. E and F are extra failure-injection
scenarios (provider outage, sudden agent loss) so those behaviours are visible in the same report.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


def constant(v: float) -> Callable[[float], float]:
    return lambda t: v


def steps(*pairs: tuple[float, float]) -> Callable[[float], float]:
    """Piecewise-constant function of time: value of the last (t0, v) with t0 <= t."""
    ordered = sorted(pairs)

    def f(t: float) -> float:
        v = ordered[0][1]
        for t0, val in ordered:
            if t >= t0:
                v = val
        return v
    return f


@dataclass(frozen=True)
class AgentDrop:
    at: int  # second at which agents vanish
    fraction: float  # of the agents currently online
    restore_at: int | None = None  # second they log back in (None = never)


@dataclass(frozen=True)
class Scenario:
    key: str
    name: str
    description: str
    answer_rate: Callable[[float], float]
    mean_talk: Callable[[float], float]
    duration: int = 600
    agents: int = 100
    wrap_up: int = 30
    outages: tuple[tuple[int, int], ...] = ()  # (start, end) seconds: provider goes dark
    agent_drops: tuple[AgentDrop, ...] = ()


SCENARIOS: dict[str, Scenario] = {s.key: s for s in [
    Scenario("A", "Low answer rate, long calls", "20% answer rate, 120 s average talk time.",
             constant(0.20), constant(120.0)),
    Scenario("B", "Medium answer rate, medium calls", "50% answer rate, 90 s average talk time.",
             constant(0.50), constant(90.0)),
    Scenario("C", "High answer rate, long calls", "70% answer rate, 180 s average talk time.",
             constant(0.70), constant(180.0)),
    Scenario("D", "Changing conditions",
             "Answer rate 60% -> 20% -> 70% -> 30% -> 50% and talk time 90 s -> 150 s -> 60 s -> 120 s "
             "while running: the pacer must follow, the Safety Controller must react to the shocks.",
             steps((0, 0.60), (200, 0.20), (400, 0.70), (600, 0.30), (750, 0.50)),
             steps((0, 90.0), (300, 150.0), (500, 60.0), (700, 120.0)), duration=900),
    Scenario("E", "Provider outage", "50% answer rate, 90 s talk; the provider goes dark from 300 s to 360 s.",
             constant(0.50), constant(90.0), duration=720, outages=((300, 360),)),
    Scenario("F", "Sudden agent loss", "50% answer rate, 90 s talk; 40% of agents vanish at 300 s, return at 420 s.",
             constant(0.50), constant(90.0), duration=720, agent_drops=(AgentDrop(300, 0.4, 420),)),
]}

DEFAULT_KEYS = ("A", "B", "C", "D")
