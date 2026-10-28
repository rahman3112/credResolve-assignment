"""Call state machine, including the provider-misbehaviour sequences from the brief."""

import itertools

import pytest

from smartdialer.states import (
    CALL_ALLOWED_TRANSITIONS,
    CALL_RANK,
    NON_TERMINAL_CALL_STATES,
    TERMINAL_CALL_STATES,
    CallDecision,
    CallState,
    IllegalTransition,
    assert_call_transition,
    decide_call_transition,
)

C = CallState
D = CallDecision


def replay(start: CallState, events: list[CallState]):
    """Apply provider events one by one the way the repository will; return final state
    and the decision for each event."""
    state, decisions = start, []
    for ev in events:
        d = decide_call_transition(state, ev)
        decisions.append(d)
        if d is D.APPLY:
            state = ev
    return state, decisions


# ---- structural invariants ----------------------------------------------------------

def test_every_state_has_rank_and_entry():
    assert set(CALL_RANK) == set(CallState) == set(CALL_ALLOWED_TRANSITIONS)


def test_all_allowed_edges_move_strictly_forward():
    # This is what makes the machine acyclic: no event sequence can loop a call.
    for src, dsts in CALL_ALLOWED_TRANSITIONS.items():
        for dst in dsts:
            assert CALL_RANK[dst] > CALL_RANK[src], (src, dst)


def test_terminal_states_are_absorbing_and_share_top_rank():
    top = max(CALL_RANK.values())
    for t in TERMINAL_CALL_STATES:
        assert CALL_ALLOWED_TRANSITIONS[t] == frozenset()
        assert CALL_RANK[t] == top
    for s in NON_TERMINAL_CALL_STATES:
        assert CALL_ALLOWED_TRANSITIONS[s], f"{s} is a dead end but not terminal"
        assert CALL_RANK[s] < top


@pytest.mark.parametrize("terminal", sorted(TERMINAL_CALL_STATES))
@pytest.mark.parametrize("target", sorted(CallState))
def test_nothing_leaves_a_terminal_state(terminal, target):
    expected = D.DUPLICATE if target == terminal else D.STALE
    assert decide_call_transition(terminal, target) is expected


def test_every_non_terminal_state_can_reach_a_terminal_state():
    for start in NON_TERMINAL_CALL_STATES:
        seen, frontier = {start}, [start]
        while frontier:
            for n in CALL_ALLOWED_TRANSITIONS[frontier.pop()]:
                if n not in seen:
                    seen.add(n)
                    frontier.append(n)
        assert seen & TERMINAL_CALL_STATES, start


def test_failure_is_always_possible_before_connect():
    # Whatever happens, a call that is not yet CONNECTED can be failed out. The reconciler
    # relies on this to clean up a call it cannot resolve.
    for s in NON_TERMINAL_CALL_STATES - {C.CONNECTED}:
        assert C.FAILED in CALL_ALLOWED_TRANSITIONS[s]


def test_decide_is_total_and_consistent_with_rank_for_all_pairs():
    for a, b in itertools.product(CallState, repeat=2):
        d = decide_call_transition(a, b)
        if d is D.APPLY:
            assert CALL_RANK[b] > CALL_RANK[a] and a not in TERMINAL_CALL_STATES
        if CALL_RANK[b] <= CALL_RANK[a] and a != b:
            assert d is D.STALE


# ---- specific decisions -------------------------------------------------------------

@pytest.mark.parametrize("src,dst", [
    (C.QUEUED, C.RESERVED),
    (C.RESERVED, C.INITIATED),
    (C.INITIATED, C.RINGING),
    (C.RINGING, C.ANSWERED),
    (C.ANSWERED, C.CONNECTED),
    (C.CONNECTED, C.COMPLETED),
    (C.QUEUED, C.INITIATED),  # predictive: no agent, so RESERVED is skipped
    (C.INITIATED, C.ANSWERED),  # RINGING event lost
])
def test_apply(src, dst):
    assert decide_call_transition(src, dst) is D.APPLY
    assert_call_transition(src, dst)


@pytest.mark.parametrize("src,dst", [
    (C.QUEUED, C.CONNECTED),
    (C.QUEUED, C.ANSWERED),
    (C.RESERVED, C.ANSWERED),
    (C.INITIATED, C.CONNECTED),  # CONNECTED needs us to bridge, never just a provider event
    (C.RINGING, C.CONNECTED),
])
def test_forward_but_not_an_edge_is_illegal(src, dst):
    assert decide_call_transition(src, dst) is D.ILLEGAL
    with pytest.raises(IllegalTransition):
        assert_call_transition(src, dst)


# ---- the sequences from the brief ---------------------------------------------------

def test_triple_answered_then_completed():
    # ANSWERED, ANSWERED, ANSWERED, COMPLETED  -> one move to ANSWERED, one to COMPLETED
    final, ds = replay(C.RINGING, [C.ANSWERED] * 3 + [C.COMPLETED])
    assert final is C.COMPLETED
    assert ds == [D.APPLY, D.DUPLICATE, D.DUPLICATE, D.APPLY]
    assert ds.count(D.APPLY) == 2  # exactly two state transitions, not four


def test_completed_before_answered_and_ringing():
    # COMPLETED, ANSWERED, RINGING (fully reversed) from INITIATED
    final, ds = replay(C.INITIATED, [C.COMPLETED, C.ANSWERED, C.RINGING])
    assert final is C.COMPLETED
    assert ds == [D.APPLY, D.STALE, D.STALE]  # late events: recorded as facts, no move


def test_ringing_arriving_after_answered_is_stale():
    final, ds = replay(C.INITIATED, [C.ANSWERED, C.RINGING])
    assert final is C.ANSWERED
    assert ds == [D.APPLY, D.STALE]


def test_late_answered_after_failure_does_not_resurrect_call():
    final, ds = replay(C.RINGING, [C.FAILED, C.ANSWERED, C.COMPLETED])
    assert final is C.FAILED
    assert ds == [D.APPLY, D.STALE, D.STALE]


def test_order_independence_of_final_state_for_a_normal_call():
    # Any permutation of a normal call's provider events ends in COMPLETED, never in an
    # earlier state, because COMPLETED has the highest rank and is absorbing.
    events = [C.RINGING, C.ANSWERED, C.COMPLETED]
    for perm in itertools.permutations(events):
        final, _ = replay(C.INITIATED, list(perm))
        assert final is C.COMPLETED, perm


def test_replaying_the_same_stream_twice_changes_nothing():
    # Idempotency: whole-stream redelivery (provider retry storm).
    events = [C.RINGING, C.ANSWERED, C.COMPLETED]
    once, _ = replay(C.INITIATED, events)
    twice, ds = replay(once, events)
    assert twice is once
    assert D.APPLY not in ds
