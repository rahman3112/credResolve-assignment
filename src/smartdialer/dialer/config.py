from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DialerConfig:
    tick_seconds: float = 1.0
    # How long a worker may hold an agent/call without any progress before the reconciler
    # (Phase 5) is allowed to look at it. Must exceed provider timeout (10s) + ring time (30s),
    # otherwise healthy slow calls get examined needlessly. Expiry is "go and look", never
    # "assume it failed".
    lease_seconds: float = 60.0
    # A conversation is long, but if its COMPLETED event is lost the agent sits in CONNECTED
    # until this lease expires and the reconciler asks the provider. So this is the worst-case
    # time an agent is wasted by a lost end-of-call event, traded against status-query load
    # (~agents_connected / connected_lease_seconds queries per second). See the scale section.
    connected_lease_seconds: float = 180.0
    permit_ttl_seconds: float = 5.0
    base_backoff_seconds: float = 30.0  # borrower retry backoff after NO_ANSWER / BUSY
    provider_retry_delay_seconds: float = 10.0  # retry delay when the provider was at fault
    # Reconciler
    reconcile_interval_seconds: float = 5.0
    reconcile_retry_seconds: float = 30.0  # provider unreachable: look again after this long
    reconcile_batch: int = 200


@dataclass(frozen=True)
class PacerConfig:
    """Knobs of the (optimistic) predictive pacer."""
    half_life_seconds: float = 120.0  # an outcome's weight halves every 2 minutes
    prior_rate: float = 0.3  # assumed answer rate before data...
    prior_weight: float = 10.0  # ...worth this many calls
    min_rate: float = 0.05  # floor, so 1/p stays finite
    max_ratio: float = 4.0  # never propose more than 4x the free agents outstanding
    default_setup_seconds: float = 15.0  # until we have measured one


@dataclass(frozen=True)
class SafetyConfig:
    """Knobs of the (pessimistic) Safety Controller. Deliberately separate from PacerConfig:
    the pacer cannot see or change these."""
    epsilon: float = 0.01  # do not START calls beyond the level where P(answers > free agents) exceeds this
    # ...but only HANG UP ringing calls beyond the (higher) level where that risk exceeds this. The gap is
    # hysteresis in probability space: stop dialing at 1%, cut only when it is materially worse (5%).
    epsilon_hangup: float = 0.25
    z: float = 2.326  # one-sided 99% for the Wilson upper bound
    long_window_seconds: float = 300.0
    short_window_seconds: float = 60.0
    abandon_window_seconds: float = 900.0  # rolling window the abandon budget is measured over
    abandon_near_fraction: float = 0.8  # fall back if ONE MORE abandon would use this much of the budget
    shock_min_samples: int = 15
    shock_min_gap: float = 0.15
    shock_sigmas: float = 3.0
    max_ratio: float = 4.0  # hard ceiling on outstanding calls / free agents, whatever the maths say
    default_credit_horizon_seconds: float = 5.0  # wrap-up credit horizon until setup times are measured
    min_credit_horizon_seconds: float = 2.0
    max_credit_horizon_seconds: float = 15.0
