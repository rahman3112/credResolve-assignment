"""The statistics behind predictive dialing. Pure functions: no I/O, no clock, easy to test.

Three tools, each answering one question:

  decayed_answer_rate   "What fraction of calls get answered, weighting recent calls more?"
                        (the pacer's EWMA: optimistic enough to be useful, adapts in minutes)
  wilson_upper          "How HIGH could the true answer rate plausibly be, given this little data?"
                        (the Safety Controller's pessimistic rate; wide when data is thin)
  max_safe_outstanding  "How many calls may be ringing at once so that the chance of more
                        borrowers answering than we have agents is <= epsilon?"
                        (binomial tail, found by binary search)

Why a binomial: if m calls are ringing and each is answered independently with probability q,
the number of answers X ~ Binomial(m, q). We need a free agent for each, so the abandon event is
{X > free_agents}. Requiring P(X > F) <= 1% at a pessimistic q makes "too many answered at once"
a rare, bounded event rather than the expected one.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime

Z_ONE_SIDED_99 = 2.326


def wilson_upper(successes: int, n: int, z: float = Z_ONE_SIDED_99) -> float:
    """Upper end of the Wilson score interval for a proportion.

    Preferred over the plain p +/- z*sqrt(p(1-p)/n) because that is useless exactly when it
    matters: with 0 answers in 5 calls it claims "0% +/- 0%". Wilson stays honest at tiny n and at
    p near 0 or 1. With n = 0 it returns 1.0 ("could be anything"), which makes the controller
    behave like progressive dialing until it has data."""
    if n <= 0:
        return 1.0
    p = successes / n
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = p + z2 / (2 * n)
    half = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    return min(1.0, (centre + half) / denom)


def _log_pmf(k: int, m: int, q: float) -> float:
    return (math.lgamma(m + 1) - math.lgamma(k + 1) - math.lgamma(m - k + 1)
            + k * math.log(q) + (m - k) * math.log1p(-q))


def binomial_tail(f: int, m: int, q: float) -> float:
    """P(X > f) for X ~ Binomial(m, q): the chance that more than f of m calls are answered."""
    if f < 0:
        return 1.0
    if f >= m or q <= 0.0:
        return 0.0
    if q >= 1.0:
        return 1.0
    # Sum whichever side has fewer terms (exact either way; the 1 - cdf side loses nothing that
    # matters at the 1e-2 thresholds we compare against).
    if m - f <= f + 1:
        return min(1.0, sum(math.exp(_log_pmf(k, m, q)) for k in range(f + 1, m + 1)))
    return max(0.0, 1.0 - sum(math.exp(_log_pmf(k, m, q)) for k in range(0, f + 1)))


def max_safe_outstanding(free_agents: int, q: float, epsilon: float = 0.01, max_ratio: float = 4.0) -> int:
    """Largest number m of simultaneously outstanding calls with P(answers > free_agents) <= epsilon.

    Properties relied on (and tested): m >= free_agents always (m = F can never overflow, which is
    exactly progressive dialing); m is non-increasing in q; non-decreasing in free_agents. The tail
    is monotone increasing in m, so binary search is exact. `max_ratio` bounds the search and is a
    hard ceiling on over-dialing whatever the statistics say."""
    if free_agents <= 0:
        return 0
    lo, hi = free_agents, max(free_agents, int(free_agents * max_ratio))
    if hi == lo or binomial_tail(free_agents, hi, q) <= epsilon:
        return hi
    while hi - lo > 1:  # invariant: tail(lo) <= epsilon < tail(hi)
        mid = (lo + hi) // 2
        if binomial_tail(free_agents, mid, q) <= epsilon:
            lo = mid
        else:
            hi = mid
    return lo


def decayed_answer_rate(outcomes: Iterable[tuple[datetime, bool]], now: datetime, half_life_seconds: float,
                        prior_rate: float, prior_weight: float) -> tuple[float, float]:
    """Exponentially time-decayed answer rate, i.e. an EWMA in continuous time: an outcome's weight
    halves every `half_life_seconds`. Returns (rate, effective_sample_size).

    Stateless on purpose: recomputed from recent outcomes each tick, so restarting a worker or
    running three of them gives the same number. `prior_weight` pseudo-calls at `prior_rate` keep a
    cold campaign (or a quiet minute) from swinging to 0% or 100% on two data points."""
    num = den = 0.0
    for t, answered in outcomes:
        w = 0.5 ** (max(0.0, (now - t).total_seconds()) / half_life_seconds)
        den += w
        num += w * (1.0 if answered else 0.0)
    return (num + prior_weight * prior_rate) / (den + prior_weight), den


def rate_shock(k_recent: int, n_recent: int, k_before: int, n_before: int, *,
               min_samples: int = 15, min_gap: float = 0.15, sigmas: float = 3.0) -> tuple[bool, dict]:
    """Has the answer rate moved by more than noise can explain? Compares the recent window with
    the data BEFORE it (excluded from the recent one, so the shock is not diluted by itself).
    Direction-agnostic: a surge breaks our over-dial assumptions (abandons), a collapse means the
    model is wrong and we are mis-pacing; in both cases we stop trusting it until it settles."""
    info = {"n_recent": n_recent, "n_before": n_before}
    if n_recent < min_samples or n_before < min_samples:
        return False, info
    p_recent, p_before = k_recent / n_recent, k_before / n_before
    sigma = math.sqrt(max(p_before * (1 - p_before), 0.01) / n_recent)
    threshold = max(min_gap, sigmas * sigma)
    info.update(p_recent=round(p_recent, 3), p_before=round(p_before, 3), threshold=round(threshold, 3))
    return abs(p_recent - p_before) > threshold, info
