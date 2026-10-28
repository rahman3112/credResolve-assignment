"""The simulated *world*: do borrowers pick up, and how long do they talk?

Shared by every mock provider (a borrower doesn't care which carrier rang them), and
mutable, so Scenario D can change answer rate / talk time mid-run and the pacer has to
cope with it.
"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass


class Outcome(enum.StrEnum):
    ANSWER = "ANSWER"
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"


@dataclass(frozen=True, slots=True)
class CallScript:
    outcome: Outcome
    ring_seconds: float  # RINGING -> ANSWERED (or -> give-up for NO_ANSWER)
    talk_seconds: float  # ANSWERED -> COMPLETED (ANSWER only)


@dataclass
class CallBehavior:
    answer_rate: float = 0.5
    mean_talk_seconds: float = 90.0
    busy_rate: float = 0.05  # of the calls that are not answered
    min_ring: float = 6.0
    max_ring: float = 24.0
    ring_timeout: float = 30.0  # unanswered calls ring this long
    min_talk: float = 5.0

    def draw(self, rng: random.Random) -> CallScript:
        if rng.random() < self.answer_rate:
            talk = max(self.min_talk, rng.expovariate(1.0 / self.mean_talk_seconds))
            return CallScript(Outcome.ANSWER, rng.uniform(self.min_ring, self.max_ring), talk)
        if rng.random() < self.busy_rate:
            return CallScript(Outcome.BUSY, 0.0, 0.0)
        return CallScript(Outcome.NO_ANSWER, self.ring_timeout, 0.0)
