"""Safety Controller, permits, pacer isolation, and the breaker feeding back into dialing."""

import dataclasses
from datetime import timedelta

import pytest

from smartdialer.dialer.pacing import Proposal, ProgressivePacer
from smartdialer.dialer.safety import Permit, PermitKind, SafetyController, Verdict
from smartdialer.providers.breaker import BreakerState, ProviderHealth
from smartdialer.providers.mock import PROFILE_A
from smartdialer.repo.campaigns import set_campaign_status
from smartdialer.repo.models import Mode
from smartdialer.repo.snapshot import load_snapshot
from smartdialer.states import AgentState

from .harness import build

pytestmark = pytest.mark.integration


def prop(n, mode=Mode.PROGRESSIVE):
    return Proposal(n, "test proposal", mode)


# ---- pacer ---------------------------------------------------------------------------

async def test_progressive_pacer_proposes_one_call_per_available_agent(pool):
    w = await build(pool, agents=7, borrowers=100)
    async with pool.acquire() as conn:
        snap = await load_snapshot(conn, w.cid, w.sched.now())
    p = ProgressivePacer().propose(snap)
    assert p.n == 7 and "7 available" in p.reason and p.details["available_agents"] == 7


async def test_pacer_is_limited_by_dialable_borrowers(pool):
    w = await build(pool, agents=7, borrowers=3)
    async with pool.acquire() as conn:
        snap = await load_snapshot(conn, w.cid, w.sched.now())
    assert ProgressivePacer().propose(snap).n == 3


def test_a_pacer_has_no_way_to_place_a_call():
    """Structural: the pacer holds no collaborators, and its only output is a number + reason."""
    pacer = ProgressivePacer()
    assert vars(pacer) == {} if hasattr(pacer, "__dict__") else True
    assert set(Proposal.__dataclass_fields__) == {"n", "reason", "mode", "details"}


# ---- verdicts ------------------------------------------------------------------------

async def test_approve_when_proposal_fits(pool):
    w = await build(pool, agents=10, borrowers=50)
    d = await w.worker.safety.review(prop(10), w.cid, w.sched.now())
    assert (d.verdict, d.approved_n) == (Verdict.APPROVE, 10) and d.permit.n == 10


async def test_reduce_uses_fresh_counts_not_the_pacers_stale_view(pool):
    """The pacer thinks 10 agents are free (stale); only 4 actually are. The controller
    re-reads the database and cuts the request."""
    w = await build(pool, agents=10, borrowers=50)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE agents SET state='PAUSED' WHERE id = ANY($1::bigint[])", w.agent_ids[:6])
    d = await w.worker.safety.review(prop(10), w.cid, w.sched.now())
    assert (d.verdict, d.approved_n, d.proposed_n) == (Verdict.REDUCE, 4, 10)
    assert "4" in d.reason and d.inputs["available_agents"] == 4


async def test_reject_when_no_agent_is_free_and_when_campaign_not_active(pool):
    w = await build(pool, agents=2, borrowers=5, online=False)
    d = await w.worker.safety.review(prop(5), w.cid, w.sched.now())
    assert d.verdict is Verdict.REJECT and d.permit is None
    async with pool.acquire() as conn:
        await set_campaign_status(conn, w.cid, "PAUSED")
    w2 = await build(pool, agents=2, borrowers=5)
    async with pool.acquire() as conn:
        await set_campaign_status(conn, w2.cid, "STOPPED")
    d = await w2.worker.safety.review(prop(2), w2.cid, w2.sched.now())
    assert d.verdict is Verdict.REJECT and "STOPPED" in d.reason


async def test_second_independent_count_can_only_lower_the_approval(pool):
    """availability says 5, but the calls table says 3 agents already hold live calls, so
    online-minus-busy is 2. Disagreement means 'dial less', never more."""
    w = await build(pool, agents=5, borrowers=20)
    async with pool.acquire() as conn:
        for i in range(3):  # live calls bound to agents whose rows (wrongly) still say AVAILABLE
            b = await conn.fetchval("SELECT id FROM borrowers WHERE campaign_id=$1 ORDER BY id OFFSET $2 LIMIT 1", w.cid, i)
            await conn.execute("UPDATE borrowers SET status='RESERVED' WHERE id=$1", b)
            await conn.execute("INSERT INTO calls (campaign_id, borrower_id, agent_id, state, attempt_no, created_at) "
                               "VALUES ($1,$2,$3,'RINGING',1,$4)", w.cid, b, w.agent_ids[i], w.sched.now())
    d = await w.worker.safety.review(prop(5), w.cid, w.sched.now())
    assert d.approved_n == 2 and d.inputs["cap_by_live_calls"] == 2 and d.inputs["cap_by_available_agents"] == 5


async def test_a_pacer_cannot_opt_a_progressive_campaign_into_predictive_dialing(pool):
    """The campaign's mode comes from the database, not from the proposal. A proposal that claims
    to be predictive on a progressive campaign is sized as progressive."""
    w = await build(pool, agents=4, borrowers=50)
    d = await w.worker.safety.review(prop(12, Mode.PREDICTIVE), w.cid, w.sched.now())
    assert (d.verdict, d.approved_n) == (Verdict.REDUCE, 4)
    assert d.permit.kind is PermitKind.PROGRESSIVE


async def test_every_decision_is_logged_with_reason_and_inputs(pool):
    w = await build(pool, agents=3, borrowers=50)
    await w.worker.safety.review(prop(10), w.cid, w.sched.now())
    await w.worker.safety.review(prop(0), w.cid, w.sched.now())  # idle tick: not logged
    rows = await w.rows("SELECT * FROM safety_decisions")
    assert len(rows) == 1
    r = rows[0]
    assert (r["verdict"], r["proposed_n"], r["approved_n"]) == ("REDUCE", 10, 3)
    assert r["reason"] and '"available_agents": 3' in r["inputs"]


# ---- permits: no bypass --------------------------------------------------------------

def test_a_permit_cannot_be_constructed_by_ordinary_code():
    import datetime
    with pytest.raises(PermissionError):
        Permit(1, 1, 100, PermitKind.PROGRESSIVE, datetime.datetime.max)


async def test_a_forged_or_foreign_permit_is_refused_by_the_allocator(pool):
    w = await build(pool, agents=5, borrowers=20)
    real = (await w.worker.safety.review(prop(5), w.cid, w.sched.now())).permit
    other = (await (await build(pool, agents=5, borrowers=20)).worker.safety.review(prop(5), w.cid, w.sched.now())).permit

    # A look-alike that reuses a live permit's id but isn't the object the controller issued.
    forged = object.__new__(Permit)
    for f in ("id", "campaign_id", "n", "kind", "expires_at", "_token"):
        object.__setattr__(forged, f, getattr(real, f))
    object.__setattr__(forged, "n", 1000)

    assert await w.worker.allocator.execute(forged) == 0
    assert await w.worker.allocator.execute(other) == 0  # issued by a different controller
    assert await w.one("SELECT count(*) FROM calls") == 0 and w.raw.stats.place_attempts == 0
    with pytest.raises(AttributeError):
        await w.worker.allocator.execute(5)  # not a permit at all


async def test_a_permit_is_single_use_limited_in_size_and_expires(pool):
    w = await build(pool, agents=10, borrowers=50)
    s = w.worker.safety
    p = (await s.review(prop(3), w.cid, w.sched.now())).permit
    assert await w.worker.allocator.execute(p) == 3  # exactly what was approved...
    assert await w.worker.allocator.execute(p) == 0  # ...and not again
    assert await w.one("SELECT count(*) FROM calls") == 3

    p2 = (await s.review(prop(3), w.cid, w.sched.now())).permit
    await w.sched.advance(60)  # past the permit's TTL
    assert await w.worker.allocator.execute(p2) == 0
    assert s.outstanding_permits == 0


# ---- provider health feeds the controller --------------------------------------------

def health_source(state, retry_at=None):
    return lambda: ProviderHealth("A", state, 1.0, 20, retry_at)


async def test_open_circuit_rejects_until_the_cooldown_then_allows_one_probe(pool):
    w = await build(pool, agents=10, borrowers=50)
    now = w.sched.now()
    s = SafetyController(pool, health_source(BreakerState.OPEN, now + timedelta(seconds=15)))
    d = await s.review(prop(10), w.cid, now)
    assert d.verdict is Verdict.REJECT and "circuit open" in d.reason

    d = await s.review(prop(10), w.cid, now + timedelta(seconds=16))  # cool-down over
    assert (d.verdict, d.approved_n) == (Verdict.REDUCE, 1) and "probe" in d.reason
    s2 = SafetyController(pool, health_source(BreakerState.HALF_OPEN))
    assert (await s2.review(prop(10), w.cid, now)).approved_n == 1


async def test_provider_timeouts_trip_the_breaker_dialing_stops_then_recovers(pool):
    bad = dataclasses.replace(PROFILE_A, place_error_rate=0.0, place_timeout_rate=1.0, timeout_but_placed_rate=0.0)
    w = await build(pool, agents=20, borrowers=300, profile=bad,
                    breaker_kw={"min_samples": 10, "open_seconds": 20, "close_after": 2})
    history = lambda: [s for _, s in w.provider.breaker.transitions]

    await w.run(30)
    assert BreakerState.OPEN in history()  # the first burst of timeouts tripped it
    reasons = {r["reason"] for r in await w.rows("SELECT reason FROM safety_decisions WHERE verdict='REJECT'")}
    assert any("circuit open" in r for r in reasons)  # and the controller said why it stopped dialing

    # While the provider stays broken only occasional single probes go out: agents are not
    # churned through reserve/fail cycles and the provider is not hammered.
    attempts, calls = w.raw.stats.place_attempts, await w.one("SELECT count(*) FROM calls")
    await w.run(20)
    assert w.raw.stats.place_attempts - attempts <= 4
    assert await w.one("SELECT count(*) FROM calls") - calls <= 4
    assert await w.one("SELECT count(*) FROM calls WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED')") <= 4

    # The provider recovers: a probe succeeds, the breaker closes, and dialing resumes.
    w.raw.profile = dataclasses.replace(PROFILE_A, place_error_rate=0.0)
    await w.run_until(lambda: _closed(w), 400)
    await w.run(60)
    h = history()
    assert h[-1] is BreakerState.CLOSED and h.index(BreakerState.OPEN) < h.index(BreakerState.HALF_OPEN) < len(h) - 1
    assert await w.one("SELECT count(*) FROM calls WHERE state = 'COMPLETED'") > 5
    # Provider failures never used up borrowers' attempts: nobody is EXHAUSTED by an outage.
    assert await w.one("SELECT count(*) FROM borrowers WHERE status = 'EXHAUSTED'") == 0
    await w.assert_invariants()


async def _closed(w):
    return w.provider.breaker.state is BreakerState.CLOSED
