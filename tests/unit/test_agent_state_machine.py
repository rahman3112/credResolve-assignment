import pytest

from smartdialer.states import (
    AGENT_ALLOWED_TRANSITIONS,
    AGENT_BUSY_STATES,
    AgentState,
    IllegalTransition,
    assert_agent_transition,
    can_agent_transition,
)

A = AgentState


def test_every_state_has_an_entry():
    assert set(AGENT_ALLOWED_TRANSITIONS) == set(AgentState)


def test_no_self_transitions():
    # A self-move would be a no-op that still bumps `version`; callers should skip it.
    for s, dsts in AGENT_ALLOWED_TRANSITIONS.items():
        assert s not in dsts


@pytest.mark.parametrize("src,dst", [
    (A.AVAILABLE, A.RESERVED),
    (A.RESERVED, A.DIALING),
    (A.DIALING, A.CONNECTED),
    (A.CONNECTED, A.WRAP_UP),
    (A.WRAP_UP, A.AVAILABLE),
    (A.RESERVED, A.AVAILABLE),  # release after failed call
    (A.DIALING, A.AVAILABLE),  # no answer
])
def test_happy_path_and_release_edges(src, dst):
    assert can_agent_transition(src, dst)
    assert_agent_transition(src, dst)


@pytest.mark.parametrize("src,dst", [
    (A.OFFLINE, A.RESERVED),  # can't dial an agent who isn't logged in
    (A.PAUSED, A.RESERVED),  # a paused agent must not be dialed
    (A.WRAP_UP, A.RESERVED),  # must finish wrap-up first
    (A.AVAILABLE, A.CONNECTED),  # must be reserved + dialed first
    (A.CONNECTED, A.AVAILABLE),  # must go through WRAP_UP
    (A.AVAILABLE, A.DIALING),  # must be reserved first
    (A.CONNECTED, A.PAUSED),  # pause-after-wrap-up instead (D-05)
])
def test_forbidden_edges(src, dst):
    assert not can_agent_transition(src, dst)
    with pytest.raises(IllegalTransition):
        assert_agent_transition(src, dst)


@pytest.mark.parametrize("busy", sorted(AGENT_BUSY_STATES))
def test_agent_can_vanish_from_any_busy_state(busy):
    # "agent disappears during call setup" (and mid-call) must always be representable.
    assert can_agent_transition(busy, A.OFFLINE)


def test_only_available_agents_can_be_reserved():
    sources = [s for s, d in AGENT_ALLOWED_TRANSITIONS.items() if A.RESERVED in d]
    assert sources == [A.AVAILABLE]


def test_every_state_is_reachable_and_can_leave():
    reachable, frontier = {A.OFFLINE}, [A.OFFLINE]
    while frontier:
        for nxt in AGENT_ALLOWED_TRANSITIONS[frontier.pop()]:
            if nxt not in reachable:
                reachable.add(nxt)
                frontier.append(nxt)
    assert reachable == set(AgentState)
    # No dead ends: unlike calls, agents are never terminal.
    assert all(AGENT_ALLOWED_TRANSITIONS[s] for s in AgentState)
