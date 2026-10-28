"""Predictive dialing: the Safety Controller's verdicts, answer-time agent binding, abandons,
hanging up excess calls, and the agent-drop reaction."""

import asyncio
from datetime import timedelta

import pytest

from smartdialer.dialer.config import SafetyConfig
from smartdialer.dialer.pacing import Proposal
from smartdialer.dialer.safety import PermitKind, SafetyController, Verdict
from smartdialer.dialer.safetymath import max_safe_outstanding, wilson_upper
from smartdialer.providers.breaker import BreakerState, ProviderHealth
from smartdialer.repo.events import ProviderEvent, ProviderEventType as T
from smartdialer.repo.models import Mode
from smartdialer.repo.reservation import reserve_predictive_call
from smartdialer.states import AgentState

from .harness import build

pytestmark = pytest.mark.integration
A = AgentState
CFG = SafetyConfig()


async def review(w, n, safety=None):
    return await (safety or w.worker.safety).review(Proposal(n, "test", Mode.PREDICTIVE), w.cid, w.sched.now())


async def steady_history(w, p=0.5):
    """Plenty of consistent evidence at answer rate p, in both the long and the short window."""
    for age, n in ((30, 120), (200, 300)):
        await w.seed_history(answered=int(n * p), unanswered=n - int(n * p), age=age)


def expected_cap(free, w_counts, epsilon=None):
    (k_s, n_s), (k_l, n_l) = w_counts
    q = max(wilson_upper(k_s, n_s, CFG.z), wilson_upper(k_l, n_l, CFG.z))
    return max_safe_outstanding(free, q, CFG.epsilon if epsilon is None else epsilon, CFG.max_ratio)


def hangup_threshold(free, w_counts):
    """Ringing calls above this are cut (risk > epsilon_hangup); above expected_cap we merely stop dialing."""
    return expected_cap(free, w_counts, CFG.epsilon_hangup)


# ---- verdicts ---------------------------------------------------------------------------

async def test_cold_start_is_progressive_until_there_is_evidence(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=10, borrowers=100)
    d = await review(w, 25)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.approved_n == 10  # one per agent, like progressive
    assert d.permit.kind is PermitKind.PROGRESSIVE  # each call reserves its agent BEFORE dialing
    assert "abandon budget" in d.reason


async def test_with_evidence_it_overdials_exactly_as_far_as_the_binomial_bound_allows(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=40, borrowers=500)
    await steady_history(w, 0.5)
    d = await review(w, 80)
    cap = expected_cap(40, ((60, 120), (210, 420)))
    assert d.permit.kind is PermitKind.PREDICTIVE and 40 < cap < 80
    assert (d.verdict, d.approved_n) == (Verdict.REDUCE, cap)  # asked for 80, allowed the safe maximum
    i = d.inputs
    assert i["free_agents"] == 40 and i["max_safe_outstanding"] == cap
    assert i["q_pessimistic"] == pytest.approx(max(i["q_upper_long"], i["q_upper_short"]))
    assert i["q_pessimistic"] > 0.5  # pessimistic: above the 50% point estimate
    for fragment in ("free agents=40", "pessimistic answer rate", "P(answers>free)<=1%", f"= {cap}", "still answerable=0"):
        assert fragment in d.reason  # the reasoning is in the log, not just the number


async def test_already_ringing_calls_use_up_the_room(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=40, borrowers=500)
    await steady_history(w)
    cap = expected_cap(40, ((60, 120), (210, 420)))
    await w.seed_ringing(cap - 5)
    d = await review(w, 80)
    assert d.approved_n == 5 and d.verdict is Verdict.REDUCE
    await w.seed_ringing(5)
    d = await review(w, 80)
    assert (d.verdict, d.approved_n, d.permit) == (Verdict.REJECT, 0, None) and "limit reached" in d.reason
    assert d.hangup_n == 0  # at the limit is fine; we just stop dialing


async def test_between_the_dial_limit_and_the_hangup_limit_we_stop_dialing_but_hang_up_nobody(pool):
    """Hysteresis in probability space: above the 1% limit we stop starting calls; we only cut calls
    once the overflow risk is above 5%. Normal churn (an answer takes an agent, lowering the limit)
    therefore never causes ghost rings."""
    w = await build(pool, mode=Mode.PREDICTIVE, agents=40, borrowers=500)
    await steady_history(w)
    cap = expected_cap(40, ((60, 120), (210, 420)))
    top = hangup_threshold(40, ((60, 120), (210, 420)))
    assert top > cap  # there is a real band
    await w.seed_ringing(top)  # at the top of the band: still no hang-ups
    d = await review(w, 80)
    assert d.approved_n == 0 and d.hangup_n == 0
    await w.seed_ringing(1)  # one past it: risk is now above 5%
    assert (await review(w, 80)).hangup_n == 1


async def test_more_agents_free_means_proportionally_more_calls_allowed(pool):
    caps = []
    for agents in (10, 40, 100):
        w = await build(pool, mode=Mode.PREDICTIVE, agents=agents, borrowers=500)
        await steady_history(w)
        caps.append((await review(w, 1000)).approved_n)
    assert caps == sorted(caps) and caps[0] >= 10 and caps[2] / 100 > caps[0] / 10  # bigger teams over-dial more efficiently


async def test_thin_evidence_means_a_wide_bound_and_no_overdial(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=20, borrowers=200)
    await w.seed_history(answered=45, unanswered=45, age=30)  # enough for the abandon budget, little for the rate
    wide = (await review(w, 100)).inputs["q_pessimistic"]
    await steady_history(w)
    tight = (await review(w, 100)).inputs["q_pessimistic"]
    assert wide > tight


async def test_abandon_budget_near_its_limit_forces_progressive(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200)
    await w.seed_history(answered=195, abandoned=5, unanswered=200, age=30)  # 5/200 = 2.5%; one more = 3%
    d = await review(w, 80)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.approved_n == 30
    assert "abandon budget near limit" in d.reason and "5 abandoned of 200 answered" in d.reason

    w2 = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200)
    await w2.seed_history(answered=199, abandoned=1, unanswered=200, age=30)  # one more = 1.0%: plenty of budget left
    assert (await review(w2, 80)).permit.kind is PermitKind.PREDICTIVE


async def test_the_budget_is_the_campaigns_not_a_hardcoded_three_percent(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200, max_abandon_rate=0.01)
    await w.seed_history(answered=199, abandoned=1, unanswered=200, age=30)  # 2/200 = 1.0% > 80% of 1%
    assert (await review(w, 80)).verdict is Verdict.FALLBACK_PROGRESSIVE


async def test_answer_rate_collapse_is_a_shock_and_forces_progressive(pool):
    """The brief's 'it predicted 70% and it dropped to 10%'."""
    w = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200)
    await w.seed_history(answered=210, unanswered=90, age=200)  # 70% before
    await w.seed_history(answered=6, unanswered=54, age=30)  # 10% over the last minute
    d = await review(w, 80)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.approved_n == 30
    assert "answer-rate shock (70% -> 10%" in d.reason


async def test_a_surge_in_answer_rate_is_a_shock_too_and_the_bound_reacts_immediately(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200)
    await w.seed_history(answered=60, unanswered=240, age=200)  # 20% before
    await w.seed_history(answered=42, unanswered=18, age=30)  # 70% now
    d = await review(w, 80)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and "shock" in d.reason
    # even without the shock rule, the short-window upper bound would already have dominated:
    assert d.inputs["q_upper_short"] > d.inputs["q_upper_long"] and d.inputs["q_pessimistic"] == d.inputs["q_upper_short"]


def health(state, retry_at=None):
    return lambda: ProviderHealth("A", state, 1.0, 20, retry_at)


async def test_unhealthy_provider_rejects_then_probes_progressively(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=20, borrowers=100)
    await steady_history(w)
    now = w.sched.now()
    opened = SafetyController(pool, health(BreakerState.OPEN, now + timedelta(seconds=15)))
    d = await review(w, 50, opened)
    assert d.verdict is Verdict.REJECT and "circuit open" in d.reason

    half = SafetyController(pool, health(BreakerState.HALF_OPEN))
    d = await review(w, 50, half)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.approved_n == 1  # one probe, deterministic style
    assert "provider circuit not closed" in d.reason and d.permit.kind is PermitKind.PROGRESSIVE


async def test_fallback_never_steals_agents_that_ringing_calls_are_counting_on(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=10, borrowers=100)  # cold start -> fallback
    await w.seed_ringing(4)
    d = await review(w, 50)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.approved_n == 6  # 10 free - 4 spoken for


# ---- excess ringing calls ---------------------------------------------------------------

async def test_material_excess_is_hung_up_newest_first_refunded_and_not_an_abandon(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=20, borrowers=200)
    await steady_history(w)
    top = hangup_threshold(20, ((60, 120), (210, 420)))
    ids = await w.seed_ringing(top + 15)  # far more ringing than 20 free agents can cover
    d = await review(w, 50)
    assert d.hangup_n == 15 and d.approved_n == 0 and d.verdict is Verdict.REJECT and "hanging up 15" in d.reason

    done = await w.worker.allocator.hang_up_excess(w.cid, d.hangup_n)
    assert done == 15
    await w.sched.advance(5)  # each pick is verified with the provider first (a round trip), then cancelled
    cancelled = {r["id"] for r in await w.rows("SELECT id FROM calls WHERE end_reason = 'HUNG_UP_EXCESS'")}
    assert cancelled == set(ids[-15:])  # the newest 15: least invested, closest to "a missed call"
    assert await w.one("SELECT count(*) FROM calls WHERE state = 'CANCELLED'") == 15
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") == 0
    b = await w.rows_one("SELECT status, attempts FROM borrowers WHERE phone = $1", f"+ring{top + 14:05d}")
    assert (b["status"], b["attempts"]) == ("PENDING", 0)  # not their fault: attempt refunded, retried shortly
    assert (await review(w, 50)).hangup_n == 0  # and the system is now within its limit


async def test_fallback_mode_hangs_up_down_to_the_agents_that_are_provably_free(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=10, borrowers=100)  # cold start: deterministic mode
    await w.seed_ringing(25)
    d = await review(w, 50)
    assert d.verdict is Verdict.FALLBACK_PROGRESSIVE and d.hangup_n == 15 and d.approved_n == 0


# ---- answer-time binding ---------------------------------------------------------------

async def _agentless_call(w) -> int:
    async with w.pool.acquire() as conn:
        res = await reserve_predictive_call(conn, w.cid, w.sched.now(), 60)
    assert res is not None and res.agent is None
    assert await w.worker.allocator._intend(res)
    return res.call.id


def ev(call_id, t, seq):
    return ProviderEvent("A", f"a-{call_id}", t, seq=seq, client_ref=call_id)


async def test_answer_binds_a_free_agent_atomically_then_completes_normally(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=2, borrowers=1)
    cid = await _agentless_call(w)
    assert await w.one("SELECT agent_id FROM calls WHERE id = $1", cid) is None  # nobody held for it
    assert {await w.agent_state(a) for a in w.agent_ids} == {A.AVAILABLE}  # and no agent was touched
    await w.worker.events.handle(ev(cid, T.RINGING, 1))
    await w.worker.events.handle(ev(cid, T.ANSWERED, 2))
    c = await w.rows_one("SELECT * FROM calls WHERE id = $1", cid)
    assert c["state"] == "CONNECTED" and c["agent_id"] in w.agent_ids
    assert await w.agent_state(c["agent_id"]) is A.CONNECTED
    assert w.worker.stats.late_bound == 1 and w.worker.stats.abandons == 0
    await w.assert_invariants()

    await w.worker.events.handle(ev(cid, T.COMPLETED, 3))
    assert await w.agent_state(c["agent_id"]) is A.WRAP_UP
    assert (await w.borrower(w.borrower_ids[0]))["status"] == "DONE"
    await w.assert_invariants()


async def test_answer_with_no_free_agent_is_recorded_as_an_abandon(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=1, borrowers=2)
    c1, c2 = await _agentless_call(w), await _agentless_call(w)  # two ringing, ONE agent
    await w.worker.events.handle(ev(c1, T.ANSWERED, 2))
    await w.worker.events.handle(ev(c2, T.ANSWERED, 2))  # the unlucky second answer
    first, second = [await w.rows_one("SELECT * FROM calls WHERE id = $1", i) for i in (c1, c2)]
    assert first["state"] == "CONNECTED"
    assert (second["state"], second["end_reason"]) == ("FAILED", "ABANDONED")
    assert second["answered_at"] is not None and second["agent_id"] is None  # the fact that matters for compliance
    assert (w.worker.stats.late_bound, w.worker.stats.abandons) == (1, 1)
    b = await w.rows_one("SELECT status, attempts FROM borrowers WHERE id = $1", w.borrower_ids[1])
    assert (b["status"], b["attempts"]) == ("PENDING", 1)  # they did answer: it counts, and they are called back later
    await w.sched.advance(3)
    assert w.raw.stats.hangups == 1  # the dead-air call is hung up at the provider
    await w.assert_invariants()


async def test_simultaneous_answers_each_get_a_different_agent_or_an_abandon_never_a_shared_agent(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=8, borrowers=20)
    ids = [await _agentless_call(w) for _ in range(20)]
    await asyncio.gather(*(w.worker.events.handle(ev(i, T.ANSWERED, 2)) for i in ids))  # 20 answers in one instant
    rows = await w.rows("SELECT state, end_reason, agent_id FROM calls ORDER BY id")
    bound = [r for r in rows if r["state"] == "CONNECTED"]
    assert len(bound) == 8 and len({r["agent_id"] for r in bound}) == 8  # 8 agents, 8 distinct calls
    assert sum(r["end_reason"] == "ABANDONED" for r in rows) == 12
    assert {await w.agent_state(a) for a in w.agent_ids} == {A.CONNECTED}
    assert w.worker.stats.late_bound == 8 and w.worker.stats.abandons == 12
    await w.assert_invariants()


async def test_the_agent_who_has_been_idle_longest_gets_the_call(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=3, borrowers=1)
    await w.one("UPDATE agents SET state_since = $2 WHERE id = $1", w.agent_ids[2], w.sched.now() - timedelta(minutes=10))
    cid = await _agentless_call(w)
    await w.worker.events.handle(ev(cid, T.ANSWERED, 2))
    assert await w.one("SELECT agent_id FROM calls WHERE id = $1", cid) == w.agent_ids[2]


async def test_duplicate_answered_events_bind_only_one_agent(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=3, borrowers=1)
    cid = await _agentless_call(w)
    for _ in range(3):
        await w.worker.events.handle(ev(cid, T.ANSWERED, 2))
    assert await w.one("SELECT count(*) FROM agents WHERE state = 'CONNECTED'") == 1
    assert w.worker.stats.late_bound == 1
    await w.assert_invariants()


# ---- the whole loop ---------------------------------------------------------------------

async def test_sudden_agent_drop_100_to_60_hangs_up_the_excess_within_one_tick_without_abandons(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=100, borrowers=1500, answer_rate=0.5, talk=120.0)
    await steady_history(w)
    await w.worker.tick()  # dial ahead for 100 free agents
    await w.sched.advance(1)  # all ringing at the provider (rings take >= 6s: nobody has answered yet)
    ringing_before = await w.one("SELECT count(*) FROM calls WHERE state IN ('INITIATED','RINGING') AND agent_id IS NULL")
    cap100 = expected_cap(100, ((60, 120), (210, 420)))
    assert 100 < ringing_before <= cap100  # over-dialing, within the 1% bound

    for a in w.agent_ids[:40]:  # 40 agents vanish within moments
        await w.worker.agents.agent_offline(a)
    t0 = w.sched.now()
    report = await w.worker.tick()  # ONE tick later: the controller decides and orders the hangups...
    assert report.decision.hangup_n > 0 and report.decision.approved_n == 0
    await w.sched.advance(2)  # ...each is verified with the provider (~0.1 s for provider A) and cancelled
    assert (w.sched.now() - t0).total_seconds() <= 2  # reaction: one tick plus a provider round trip

    ringing_after = await w.one("SELECT count(*) FROM calls WHERE state IN ('INITIATED','RINGING') AND agent_id IS NULL")
    top = hangup_threshold(60, ((60, 120), (210, 420)))  # the 5% level for 60 agents
    assert top - 3 <= ringing_after <= top  # cut back to it (a few BUSY calls also end naturally in those 2 s)
    assert ringing_before - ringing_after >= report.decision.hangup_n
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'HUNG_UP_EXCESS'") == report.decision.hangup_n
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") == 0  # nobody was left waiting

    await w.run(90)  # and the campaign carries on safely with the 60 agents left
    assert await w.one("SELECT count(*) FROM calls WHERE end_reason = 'ABANDONED'") <= 2
    await w.assert_invariants()


async def _utilisation(w, agents, seconds):
    """Share of agent-seconds spent connected, plus the abandon rate and call counts."""
    r = await w.rows_one(
        "SELECT coalesce(sum(extract(epoch FROM (coalesce(ended_at, $2) - connected_at))), 0) AS connected_s, "
        "count(*) FILTER (WHERE answered_at IS NOT NULL) AS answered, "
        "count(*) FILTER (WHERE end_reason = 'ABANDONED') AS abandoned, count(*) AS calls "
        "FROM calls WHERE campaign_id = $1 AND (connected_at IS NOT NULL OR end_reason = 'ABANDONED' OR answered_at IS NOT NULL OR true)",
        w.cid, w.sched.now())
    started = await w.one("SELECT count(*) FROM calls WHERE campaign_id = $1", w.cid)
    return {"util": float(r["connected_s"]) / (agents * seconds), "answered": r["answered"],
            "abandoned": r["abandoned"], "calls": started,
            "abandon_rate": r["abandoned"] / max(1, r["answered"])}


async def test_predictive_beats_progressive_on_utilisation_within_the_abandon_budget(pool):
    agents, seconds = 60, 600
    results = {}
    for mode in (Mode.PROGRESSIVE, Mode.PREDICTIVE):
        w = await build(pool, mode=mode, agents=agents, borrowers=3000, answer_rate=0.4, talk=60.0, wrap_up=15, seed=3,
                        provider_name=f"A{mode.value[:4]}")
        await w.run(seconds, check=False)
        await w.assert_invariants()
        results[mode] = await _utilisation(w, agents, seconds)
        results[mode]["decisions"] = dict(w.worker.stats.decisions)
    prog, pred = results[Mode.PROGRESSIVE], results[Mode.PREDICTIVE]
    note = f"progressive={prog} predictive={pred}"
    print("COMPARISON", note)
    assert prog["abandoned"] == 0  # progressive: zero by construction
    assert pred["abandon_rate"] <= 0.03, note  # the compliance line
    assert pred["util"] > prog["util"], note  # and the point of the exercise


async def test_stale_rings_do_not_trigger_hangups_or_use_up_dialing_room(pool):
    """Calls that have rung longer than the slowest answer we have measured (here 10 s) are no-answers
    waiting out their timeout. They must not be counted as about-to-be-answered, or the controller hangs
    up healthy calls every time answers consume agents."""
    w = await build(pool, mode=Mode.PREDICTIVE, agents=40, borrowers=500)
    await steady_history(w)  # answered calls took 10 s each -> p99 = 10 s
    cap = expected_cap(40, ((60, 120), (210, 420)))
    await w.seed_ringing(cap + 30, age=20)  # 20 s old: older than any answer ever seen
    d = await review(w, 80)
    assert d.inputs["answer_age_p99"] == pytest.approx(10.0) and d.inputs["ringing_young"] == 0
    assert d.inputs["ringing_agentless"] == cap + 30
    assert d.hangup_n == 0 and d.approved_n == cap  # full room: the stale rings cost nothing

    await w.seed_ringing(5, age=2)  # fresh ones DO count
    assert (await review(w, 80)).approved_n == cap - 5


# ---- never hang up on someone who has already answered ------------------------------------

async def test_a_call_the_provider_says_is_already_answered_is_not_hung_up_it_is_bound_to_an_agent(pool):
    """Our database still says RINGING because the ANSWERED callback is in flight (a slow provider).
    Cancelling it would be dead air. The hangup path asks the provider first, finds the truth, and feeds it
    through the normal event path instead: the borrower gets an agent."""
    from smartdialer.providers.base import PlaceCallRequest
    w = await build(pool, mode=Mode.PREDICTIVE, agents=2, borrowers=1, answer_rate=1.0)
    w.behavior.mean_talk_seconds, w.behavior.min_talk = 0.001, 300.0
    cid = await _agentless_call(w)
    w.raw.set_event_handler(None)  # callbacks are "in flight": the dialer has not heard anything yet
    await w.sched.run(w.raw.place_call(PlaceCallRequest(cid, "+15550000000")))
    await w.sched.advance(40)  # the borrower answered at the provider long ago
    assert (await w.rows_one("SELECT state FROM calls WHERE id = $1", cid))["state"] == "INITIATED"  # we are behind

    await w.worker.allocator.hang_up_excess(w.cid, 1)
    await w.sched.advance(5)
    c = await w.rows_one("SELECT state, end_reason, agent_id FROM calls WHERE id = $1", cid)
    assert c["state"] == "CONNECTED" and c["end_reason"] is None and c["agent_id"] in w.agent_ids  # bridged, not cancelled
    assert w.worker.stats.hangups_saved == 1 and w.worker.stats.hung_up_excess == 0
    assert w.raw.stats.hangups_on_answered == 0  # ground truth: nobody was hung up on
    await w.assert_invariants()


async def test_if_the_provider_cannot_confirm_the_call_is_only_ringing_nothing_is_hung_up(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=2, borrowers=1)
    await w.seed_ringing(1)
    w.raw.start_outage()  # cannot ask: do not guess
    await w.worker.allocator.hang_up_excess(w.cid, 1)
    await w.sched.advance(30)
    assert await w.one("SELECT count(*) FROM calls WHERE state = 'RINGING'") == 1
    assert w.worker.stats.hangups_unverified == 1 and w.worker.stats.hung_up_excess == 0


async def test_calls_already_being_verified_are_not_picked_twice(pool):
    w = await build(pool, mode=Mode.PREDICTIVE, agents=2, borrowers=10)
    await w.seed_ringing(3)
    first = await w.worker.allocator.hang_up_excess(w.cid, 3)
    second = await w.worker.allocator.hang_up_excess(w.cid, 3)  # next tick, verification still in progress
    assert (first, second) == (3, 0)
    await w.sched.advance(5)
    assert w.worker.stats.hung_up_excess == 3


async def test_dead_air_hangups_count_against_the_abandon_budget(pool):
    """If a call we cancelled turns out to have been answered, it is an abandon for compliance purposes,
    even though its row says HUNG_UP_EXCESS: the abandon budget must see it."""
    w = await build(pool, mode=Mode.PREDICTIVE, agents=5, borrowers=10)
    await w.seed_history(answered=100, unanswered=100, age=30)
    (cid,) = await w.seed_ringing(1)
    await w.one("UPDATE calls SET state='CANCELLED', end_reason='HUNG_UP_EXCESS', ended_at=$2 WHERE id=$1", cid, w.sched.now())
    await w.one("INSERT INTO provider_events (provider, provider_call_id, event_type, seq, call_id, received_at, outcome) "
                "VALUES ('A', 'late', 'ANSWERED', 1, $1, $2, 'STALE')", cid, w.sched.now())  # the late callback
    d = await review(w, 10)
    assert d.inputs["abandoned_window"] == 1 and d.inputs["answered_window"] == 101


async def test_a_burst_of_fast_answers_is_not_mistaken_for_a_surge(pool):
    """Censoring bias. After a burst of dialing, the calls that WILL be answered resolve first (6-24 s); the
    no-answers are still ringing. Counted by resolution time, the recent window would be ~100% answered and
    the shock detector would call it a surge. Statistics are by DIAL cohort and only count a call once its whole
    cohort has had time to resolve, so the burst is invisible until it can be judged fairly."""
    w = await build(pool, mode=Mode.PREDICTIVE, agents=30, borrowers=200)
    await w.seed_history(answered=60, unanswered=240, age=200)  # a settled 20% before
    await w.seed_history(answered=40, unanswered=0, age=10)  # dialed 20 s ago: only the fast answers have resolved so far
    d = await review(w, 50)
    assert d.inputs["resolved_short"] == 0 and "shock" not in d.reason  # too fresh to count; no false surge
    assert d.inputs["q_upper_long"] < 0.4  # and the pessimistic rate was not dragged up by half the evidence

    await w.sched.advance(20)  # the cohort is now 40 s old: its (still all-answered, in this test) outcomes count
    d = await review(w, 50)
    assert d.inputs["resolved_short"] >= 40
