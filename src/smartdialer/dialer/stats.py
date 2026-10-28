from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass
class DialerStats:
    """Plain counters for reports and tests. Not a source of truth (the DB is)."""
    ticks: int = 0
    reserved: int = 0
    placed: int = 0
    rejected: int = 0
    provider_errors: int = 0
    circuit_open: int = 0
    timeouts_resolved_placed: int = 0
    timeouts_not_placed: int = 0
    timeouts_unresolved: int = 0  # left for the reconciler (never guessed)
    compensating_hangups: int = 0
    hangup_failures: int = 0
    late_bound: int = 0  # predictive: an agent was found at the moment of answer
    abandons: int = 0  # predictive: answered with no agent free (the compliance event)
    hung_up_excess: int = 0  # still-ringing calls cancelled because too many were outstanding
    hangups_saved: int = 0  # picked for hangup but the provider said they were already answered/finished: NOT cancelled
    hangups_unverified: int = 0  # provider unreachable: could not confirm the call was only ringing, so left alone
    dial_skipped_taken_over: int = 0  # someone else (agent-offline, reconciler) owned the call first
    events_applied: Counter = field(default_factory=Counter)  # EventOutcome -> count
    decisions: Counter = field(default_factory=Counter)  # Verdict -> count
