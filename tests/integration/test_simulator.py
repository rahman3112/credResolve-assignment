"""The simulator itself: it runs, it is reproducible, and its numbers add up."""

import asyncio
import dataclasses

import pytest

from smartdialer.sim import report
from smartdialer.sim.dbutil import ensure_sim_database
from smartdialer.sim.runner import RunSpec, run_scenario
from smartdialer.sim.scenarios import SCENARIOS, steps

pytestmark = pytest.mark.integration


def small(scenario="B", provider="A", mode="PREDICTIVE", **kw):
    return RunSpec(scenario, provider, mode, agents=kw.pop("agents", 20), duration=kw.pop("duration", 150),
                   db_name=kw.pop("db_name", "dialer_test_sim"), **kw)


@pytest.fixture(autouse=True, scope="module")
def _db():
    try:
        asyncio.run(ensure_sim_database("dialer_test_sim"))
    except OSError:
        pytest.skip("PostgreSQL not reachable")


def test_scenarios_match_the_briefs_table():
    a, b, c = SCENARIOS["A"], SCENARIOS["B"], SCENARIOS["C"]
    assert (a.answer_rate(0), a.mean_talk(0)) == (0.20, 120.0)
    assert (b.answer_rate(0), b.mean_talk(0)) == (0.50, 90.0)
    assert (c.answer_rate(0), c.mean_talk(0)) == (0.70, 180.0)
    d = SCENARIOS["D"]  # "changing": both quantities really move, including a collapse and a surge
    rates = {d.answer_rate(t) for t in range(0, d.duration, 10)}
    talks = {d.mean_talk(t) for t in range(0, d.duration, 10)}
    assert len(rates) >= 4 and len(talks) >= 3 and min(rates) <= 0.2 and max(rates) >= 0.7


def test_steps_helper_is_piecewise_constant():
    f = steps((0, 1.0), (10, 2.0), (20, 3.0))
    assert [f(t) for t in (0, 9.9, 10, 19, 20, 99)] == [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]


def test_a_run_produces_every_number_the_brief_asks_for():
    r = asyncio.run(run_scenario(small()))
    assert {"utilization", "calls", "pacing", "safety", "provider_incidents", "series"} <= set(r)
    assert 0 < r["utilization"]["connected"] < 1
    c = r["calls"]
    assert c["initiated"] >= c["answered"] >= c["connected"] > 0  # funnel is ordered
    assert c["abandoned"] <= c["answered"] and 0 <= c["abandon_rate"] <= 1
    assert sum(r["safety"]["decisions"].values()) > 0
    u = r["utilization"]
    assert u["connected"] + u["waiting"] + u["wrap_up"] == pytest.approx(1.0, abs=0.02)  # every agent-second is somewhere
    assert len(r["series"]) == 15 and r["series"][0]["t"] == 0


def test_same_seed_same_run_exactly():
    """Reproducibility: virtual clock + order-independent seeded randomness => identical results,
    even though the database I/O underneath finishes in a different order each time."""
    spec = small(provider="B", duration=120)
    a, b = asyncio.run(run_scenario(spec)), asyncio.run(run_scenario(spec))
    for r in (a, b):
        r.pop("wall_seconds")
    assert a == b


def test_a_different_seed_gives_a_different_run():
    a = asyncio.run(run_scenario(small(duration=120)))
    b = asyncio.run(run_scenario(small(duration=120, seed=2)))
    assert a["calls"] != b["calls"]


def test_progressive_never_abandons_and_never_over_dials():
    r = asyncio.run(run_scenario(small(mode="PROGRESSIVE")))
    assert r["calls"]["abandoned"] == 0 and r["calls"]["hung_up_excess"] == 0
    assert r["pacing"]["max_ringing_agentless"] == 0  # no call ever rang without an agent held for it
    assert set(r["safety"]["decisions"]) <= {"APPROVE", "REDUCE", "REJECT"}


def test_outage_scenario_exercises_breaker_and_reconciler_and_recovers():
    r = asyncio.run(run_scenario(RunSpec("E", "A", "PREDICTIVE", agents=20, duration=480, db_name="dialer_test_sim")))
    inc = r["provider_incidents"]
    assert inc["stats"]["timeouts"] > 0  # the provider really went dark
    assert inc["reconciler"]["examined"] > 0  # and the reconciler had work to do afterwards
    assert r["calls"]["connected"] > 0
    last = r["series"][-1]
    assert last["connected"] > 0  # dialing recovered: agents are on calls again at the end


def test_agent_loss_scenario_drops_and_restores_agents():
    r = asyncio.run(run_scenario(RunSpec("F", "A", "PREDICTIVE", agents=30, duration=480, db_name="dialer_test_sim")))
    online = {p["t"]: p["online"] for p in r["series"]}
    assert online[290] == 30 and online[310] == 18 and online[430] == 30  # 40% gone at 300 s, back at 420 s
    assert r["calls"]["abandon_rate"] <= 0.03


def test_report_renders_both_modes_and_the_comparison():
    results = [asyncio.run(run_scenario(small(mode=m, duration=100))) for m in ("PROGRESSIVE", "PREDICTIVE")]
    md = report.markdown(results)
    assert "Scenario B" in md and "Predictive vs progressive" in md and "Util (>3 min)" in md
    assert "Progressive" in report.console(results)
