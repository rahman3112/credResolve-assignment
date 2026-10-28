from datetime import datetime, timedelta, timezone

from smartdialer.dialer.config import PacerConfig
from smartdialer.dialer.pacing import PredictivePacer
from smartdialer.repo.models import Mode
from smartdialer.repo.snapshot import CampaignSnapshot, PredictiveInputs
from smartdialer.states import AgentState, CallState

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def snap(*, available=10, wraps=(), ringing=0, outcomes=(), setup=15.0, dialable=1000, online_busy=0):
    agents = {s: 0 for s in AgentState}
    agents[AgentState.AVAILABLE] = available
    agents[AgentState.WRAP_UP] = len(wraps)
    agents[AgentState.CONNECTED] = online_busy
    return CampaignSnapshot(
        1, Mode.PREDICTIVE, "ACTIVE", NOW, agents, {CallState.RINGING: ringing}, online_busy, dialable,
        agentless_live_calls=ringing, hangable_calls=ringing,
        predictive=PredictiveInputs(tuple(outcomes), setup, tuple(NOW + timedelta(seconds=s) for s in wraps)))


def history(rate, n=400, step=1.0, start=0.0):
    """n resolved calls, evenly spread over the past, with the answered ones interleaved evenly
    (dithered) so every recent stretch has the right fraction, not just the whole."""
    return [(NOW - timedelta(seconds=start + i * step), int((i + 1) * rate) > int(i * rate)) for i in range(n)]


def test_docstring_example_10_free_at_50_percent_with_12_ringing_proposes_8():
    p = PredictivePacer(PacerConfig(prior_weight=0.0001)).propose(snap(available=10, ringing=12, outcomes=history(0.5)))
    assert p.n == 8 and p.details["target_outstanding"] == 20


def test_formula_floor_free_over_p_minus_ringing():
    pacer = PredictivePacer(PacerConfig(prior_weight=0.0001))
    s = snap(available=7, ringing=0, outcomes=history(0.3))
    p = pacer.propose(s)
    assert p.details["target_outstanding"] == int(7 / p.details["answer_rate"]) and p.n == p.details["target_outstanding"]


def test_wrap_ups_ending_within_setup_time_count_as_free_others_do_not():
    pacer = PredictivePacer(PacerConfig(prior_weight=0.0001))
    s = snap(available=2, wraps=[3, 10, 14.9, 15.1, 60], setup=15.0, outcomes=history(0.5))
    p = pacer.propose(s)
    assert p.details["wrap_ups_soon"] == 3 and p.details["free"] == 5


def test_never_negative_when_already_over_target():
    assert PredictivePacer().propose(snap(available=2, ringing=50, outcomes=history(0.5))).n == 0


def test_no_free_agents_proposes_nothing_and_says_why():
    p = PredictivePacer().propose(snap(available=0, wraps=[100]))
    assert p.n == 0 and "no free agents" in p.reason


def test_ratio_ceiling_and_rate_floor_prevent_absurd_proposals():
    p = PredictivePacer().propose(snap(available=10, outcomes=history(0.0)))  # nobody ever answers
    assert p.details["answer_rate"] >= 0.05 and p.details["target_outstanding"] <= 40


def test_limited_by_dialable_borrowers_and_explains_it():
    p = PredictivePacer().propose(snap(available=10, dialable=3, outcomes=history(0.5)))
    assert p.n == 3 and "limited to 3 by dialable borrowers" in p.reason


def test_every_proposal_carries_its_reasoning():
    p = PredictivePacer().propose(snap(available=10, ringing=4, outcomes=history(0.5)))
    for fragment in ("10 free agents", "answer rate", "target", "4 already ringing"):
        assert fragment in p.reason
    assert {"free", "answer_rate", "target_outstanding", "already_ringing", "setup_seconds"} <= set(p.details)


def test_cold_start_uses_the_prior_not_a_division_by_zero():
    p = PredictivePacer().propose(snap(available=10, outcomes=()))
    assert p.details["answer_rate"] == 0.3 and p.n == 33


def test_answer_rate_estimate_follows_a_changing_world():
    pacer = PredictivePacer()
    rate = lambda o: pacer.propose(snap(available=10, outcomes=o)).details["answer_rate"]
    before = history(0.7, n=600)
    assert 0.65 < rate(before) < 0.75
    # The world drops to 10%. The EWMA (half-life 2 min) is deliberately not twitchy:
    after_1min = history(0.1, n=60) + history(0.7, n=540, start=60)
    after_5min = history(0.1, n=300) + history(0.7, n=300, start=300)
    assert rate(after_1min) < rate(before) - 0.15  # clearly moving after a minute...
    assert rate(after_5min) < 0.3  # ...and has adapted after five. (The Safety Controller's
    #                                 shock detector covers the gap in between.)


def test_pacer_is_pure_and_holds_nothing():
    pacer = PredictivePacer()
    s = snap(available=5, outcomes=history(0.4))
    assert pacer.propose(s) == pacer.propose(s)  # same input -> same output, no hidden state
    assert set(vars(pacer)) == {"cfg"}  # no pool, provider, allocator or safety reference


# ---- stale rings -------------------------------------------------------------------------

def snap_with_ages(ages, p99, ringing=None):
    base = snap(available=10, ringing=len(ages) if ringing is None else ringing, outcomes=history(0.5))
    pi = base.predictive
    import dataclasses
    return dataclasses.replace(base, predictive=dataclasses.replace(pi, answer_age_p99=p99, agentless_ages=tuple(ages)))


def test_calls_that_rang_longer_than_any_observed_answer_are_not_counted_as_answerable():
    s = snap_with_ages([1, 5, 12, 20, 26, 29], p99=24.0)
    assert s.agentless_live_calls == 6 and s.young_agentless() == 4  # the 26 s and 29 s rings are waiting out a timeout


def test_without_enough_data_every_ringing_call_counts_up_to_the_30s_timeout():
    s = snap_with_ages([1, 5, 12, 20, 26, 29], p99=None)
    assert s.young_agentless() == 6  # no measured p99 yet: do not discount anything


def test_pacer_does_not_let_stale_rings_suppress_new_dialing():
    pacer = PredictivePacer(PacerConfig(prior_weight=0.0001))
    fresh = pacer.propose(snap_with_ages([1] * 10, p99=24.0))
    stale = pacer.propose(snap_with_ages([28] * 10, p99=24.0))
    assert stale.n == fresh.n + 10  # ten stale rings no longer count against the target
    assert stale.details["already_ringing"] == 0
