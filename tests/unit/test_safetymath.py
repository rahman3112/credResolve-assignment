"""The statistics that decide how aggressively we may dial. Checked against exact enumeration,
known values and the properties the Safety Controller relies on."""

import math
from datetime import datetime, timedelta, timezone

import pytest

from smartdialer.dialer.safetymath import (
    binomial_tail,
    decayed_answer_rate,
    max_safe_outstanding,
    rate_shock,
    wilson_upper,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


# ---- binomial tail ------------------------------------------------------------------

def brute_tail(f, m, q):
    return sum(math.comb(m, k) * q**k * (1 - q) ** (m - k) for k in range(f + 1, m + 1))


@pytest.mark.parametrize("f,m,q", [(0, 1, 0.5), (3, 10, 0.5), (10, 20, 0.5), (10, 15, 0.7), (50, 80, 0.3),
                                   (5, 40, 0.1), (100, 150, 0.6), (0, 30, 0.02)])
def test_tail_matches_exact_enumeration(f, m, q):
    assert binomial_tail(f, m, q) == pytest.approx(brute_tail(f, m, q), rel=1e-9, abs=1e-12)


def test_tail_known_values():
    assert binomial_tail(0, 1, 0.5) == pytest.approx(0.5)  # one ringing call, no agent: coin flip
    assert binomial_tail(1, 2, 0.5) == pytest.approx(0.25)  # both of 2 answer
    assert binomial_tail(10, 20, 0.5) == pytest.approx(0.4119, abs=1e-4)


def test_tail_edges():
    assert binomial_tail(5, 5, 0.9) == 0.0  # can't have more answers than calls
    assert binomial_tail(10, 5, 0.9) == 0.0
    assert binomial_tail(3, 10, 0.0) == 0.0 and binomial_tail(3, 10, 1.0) == 1.0
    assert binomial_tail(-1, 3, 0.2) == 1.0


def test_tail_is_monotone_in_calls_and_rate_and_agents():
    for q in (0.1, 0.5, 0.8):
        tails = [binomial_tail(10, m, q) for m in range(10, 60)]
        assert tails == sorted(tails)  # more calls ringing -> more risk
    for m in (20, 40):
        assert [binomial_tail(10, m, q / 10) for q in range(1, 10)] == sorted(binomial_tail(10, m, q / 10) for q in range(1, 10))
        assert [binomial_tail(f, m, 0.5) for f in range(0, m)] == sorted((binomial_tail(f, m, 0.5) for f in range(0, m)), reverse=True)


def test_tail_is_stable_for_large_numbers():
    t = binomial_tail(8000, 10000, 0.7)
    assert 0.0 <= t <= 1.0 and not math.isnan(t)
    assert binomial_tail(7000, 10000, 0.7) == pytest.approx(0.5, abs=0.05)  # mean 7000


# ---- max safe outstanding -----------------------------------------------------------

def test_never_below_free_agents_that_is_progressive_dialing():
    for free in (1, 5, 50):
        for q in (0.0, 0.01, 0.5, 0.99, 1.0):
            assert max_safe_outstanding(free, q) >= free


def test_certain_answers_means_no_overdial_and_certain_silence_means_the_ceiling():
    assert max_safe_outstanding(10, 1.0) == 10
    assert max_safe_outstanding(10, 0.0, max_ratio=4.0) == 40


def test_result_is_exactly_the_largest_m_within_epsilon():
    for free, q in [(10, 0.5), (20, 0.3), (50, 0.2), (100, 0.7), (7, 0.9)]:
        m = max_safe_outstanding(free, q, epsilon=0.01, max_ratio=10)
        assert binomial_tail(free, m, q) <= 0.01
        if m < free * 10:
            assert binomial_tail(free, m + 1, q) > 0.01  # one more call would break the promise


def test_lower_answer_rate_allows_more_calls_and_more_agents_allow_more_calls():
    ms = [max_safe_outstanding(20, q / 10) for q in range(1, 10)]
    assert ms == sorted(ms, reverse=True)
    assert [max_safe_outstanding(f, 0.5) for f in (5, 10, 20, 40)] == sorted(max_safe_outstanding(f, 0.5) for f in (5, 10, 20, 40))


def test_overdial_is_below_naive_pacing_and_pooling_makes_large_teams_more_efficient():
    """Naive pacing would put free/q calls out (2x at q=0.5). A 1% overflow promise allows less, and
    the gap closes as the team grows (law of large numbers): why big floors benefit most."""
    efficiency = {}
    for free in (5, 20, 100, 500):
        m = max_safe_outstanding(free, 0.5, max_ratio=10)
        assert free <= m < free / 0.5  # never below progressive, always below the naive free/q
        efficiency[free] = m / (free / 0.5)  # share of the naive over-dial we can safely take
    assert max_safe_outstanding(5, 0.5) == 5  # a 5-agent team cannot safely over-dial AT ALL:
    #                                           one extra call already has a 1.6% chance of overflow
    assert max_safe_outstanding(20, 0.5) > 20
    assert list(efficiency.values()) == sorted(efficiency.values())
    assert efficiency[5] < 0.55 and efficiency[500] > 0.9


def test_ceiling_is_a_hard_limit():
    assert max_safe_outstanding(10, 0.01, max_ratio=2.0) == 20


def test_zero_agents_means_zero_calls():
    assert max_safe_outstanding(0, 0.3) == 0


# ---- Wilson ---------------------------------------------------------------------------

def test_no_data_means_could_be_anything():
    assert wilson_upper(0, 0) == 1.0  # -> m == free: behaves like progressive until it has evidence


def test_wilson_is_honest_with_tiny_samples_where_the_naive_interval_is_not():
    assert wilson_upper(0, 5) > 0.5  # naive interval would say 0% +/- 0%
    assert wilson_upper(0, 5) > wilson_upper(0, 50) > wilson_upper(0, 500) > 0.0


def test_wilson_upper_bounds_the_point_estimate_and_tightens_with_data():
    for k, n in [(1, 10), (5, 10), (50, 100), (500, 1000)]:
        assert wilson_upper(k, n) > k / n
    assert wilson_upper(500, 1000) - 0.5 < wilson_upper(5, 10) - 0.5
    assert wilson_upper(10, 10) == pytest.approx(1.0)


def test_wilson_reference_value():
    # k=1, n=200, z=2.326: ~3.5% (worked by hand in docs/DECISIONS.md)
    assert wilson_upper(1, 200) == pytest.approx(0.0354, abs=0.001)


# ---- decayed answer rate (the pacer's EWMA) ---------------------------------------------

def outcomes(*pairs):  # (seconds_ago, answered)
    return [(NOW - timedelta(seconds=s), a) for s, a in pairs]


def test_no_data_returns_the_prior():
    p, n = decayed_answer_rate([], NOW, 120, prior_rate=0.3, prior_weight=10)
    assert (p, n) == (0.3, 0.0)


def test_recent_calls_dominate_old_ones():
    old_yes = outcomes(*[(600, True)] * 50)  # five half-lives ago: nearly forgotten
    new_no = outcomes(*[(5, False)] * 20)
    p, _ = decayed_answer_rate(old_yes + new_no, NOW, 120, 0.3, 1)
    assert p < 0.1


def test_converges_to_the_true_rate_with_data():
    data = outcomes(*[(i, i % 2 == 0) for i in range(0, 300)])  # 50% answered
    p, n = decayed_answer_rate(data, NOW, 120, 0.3, 10)
    assert p == pytest.approx(0.5, abs=0.03) and n > 100


def test_prior_stops_two_data_points_from_swinging_the_estimate():
    p, _ = decayed_answer_rate(outcomes((1, True), (2, True)), NOW, 120, prior_rate=0.3, prior_weight=10)
    assert 0.3 < p < 0.5  # not 100%


# ---- shock ------------------------------------------------------------------------------

def test_no_shock_when_rates_agree_or_data_is_thin():
    assert rate_shock(30, 60, 150, 300)[0] is False
    assert rate_shock(0, 5, 150, 300)[0] is False  # 5 recent calls is not evidence
    assert rate_shock(30, 60, 4, 8)[0] is False


def test_collapse_and_surge_are_both_shocks():
    assert rate_shock(6, 60, 210, 300)[0] is True  # 70% -> 10%
    assert rate_shock(42, 60, 60, 300)[0] is True  # 20% -> 70%


def test_ordinary_noise_is_not_a_shock():
    # 50% vs a recent 58% on 60 calls: within sampling noise
    assert rate_shock(35, 60, 120, 240)[0] is False
