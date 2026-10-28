"""Agent and call state machines.

These two tables are the *only* place that says what moves are legal. Repository code
(Phase 2) calls the decide_* functions before every UPDATE, and the UPDATE itself is a
compare-and-set on `version`, so two writers can never both apply a move from the same
starting state.

Design rules (see docs/DECISIONS.md D-03, D-04):
  * Call states have a RANK. A provider event may only move a call to a strictly higher
    rank. Anything else is a late/duplicate event: it is recorded as a fact but changes
    nothing.
  * Terminal call states are ABSORBING. Nothing leaves them, including another terminal.
  * Ranks may be skipped (INITIATED -> ANSWERED when the RINGING event was lost or
    reordered) but only along edges listed in CALL_ALLOWED_TRANSITIONS.
"""

from __future__ import annotations

import enum


class AgentState(enum.StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"  # claimed for one call; no provider call placed yet
    DIALING = "DIALING"  # progressive: the agent's call is ringing
    CONNECTED = "CONNECTED"  # talking to a borrower
    WRAP_UP = "WRAP_UP"  # after-call work; becomes AVAILABLE when wrap_up_until passes
    PAUSED = "PAUSED"  # agent chose a break


class CallState(enum.StrEnum):
    QUEUED = "QUEUED"  # row exists, borrower claimed (partial unique index)
    RESERVED = "RESERVED"  # an agent is held for this call (progressive only)
    INITIATED = "INITIATED"  # place_call accepted by the provider
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"  # borrower picked up; no agent bridged yet
    CONNECTED = "CONNECTED"  # borrower bridged to an agent
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"  # incl. abandons: end_reason says why
    CANCELLED = "CANCELLED"  # we hung up before the borrower answered


# --------------------------------------------------------------------------------------
# Agents
# --------------------------------------------------------------------------------------

# Each edge is annotated with the event that causes it, because "why can this happen?"
# is the question an interviewer asks.
AGENT_ALLOWED_TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.OFFLINE: frozenset({
        AgentState.AVAILABLE,  # agent logs in
        AgentState.PAUSED,  # agent logs in on a break
    }),
    AgentState.AVAILABLE: frozenset({
        AgentState.RESERVED,  # dialer claims the agent (atomic, SKIP LOCKED)
        AgentState.PAUSED,  # agent starts a break
        AgentState.OFFLINE,  # agent logs out / disappears
    }),
    AgentState.RESERVED: frozenset({
        AgentState.DIALING,  # provider accepted the call (progressive)
        AgentState.CONNECTED,  # predictive: reserved *because* the borrower already answered
        AgentState.AVAILABLE,  # call failed before dialing, or lease expired: release
        AgentState.OFFLINE,  # agent disappeared during call setup
    }),
    AgentState.DIALING: frozenset({
        AgentState.CONNECTED,  # borrower answered and was bridged
        AgentState.AVAILABLE,  # no answer / busy / provider failure: release
        AgentState.OFFLINE,  # agent disappeared during call setup
    }),
    AgentState.CONNECTED: frozenset({
        AgentState.WRAP_UP,  # call ended
        AgentState.OFFLINE,  # agent dropped mid-call (call fails, agent not reusable)
    }),
    AgentState.WRAP_UP: frozenset({
        AgentState.AVAILABLE,  # wrap-up timer elapsed
        AgentState.PAUSED,  # agent took a break after the call
        AgentState.OFFLINE,
    }),
    AgentState.PAUSED: frozenset({
        AgentState.AVAILABLE,  # break over
        AgentState.OFFLINE,
    }),
}
# Deliberately absent: PAUSED from RESERVED/DIALING/CONNECTED. A busy agent who asks for a
# break is handled as "pause after wrap-up" (D-05), so a break can never strand a call.
# OFFLINE *is* reachable from every state: an agent can vanish at any moment.

# States in which an agent is committed to a call. Used for "never more agent-bound calls
# than agents" invariants.
AGENT_BUSY_STATES = frozenset({
    AgentState.RESERVED,
    AgentState.DIALING,
    AgentState.CONNECTED,
})


def can_agent_transition(current: AgentState, target: AgentState) -> bool:
    return target in AGENT_ALLOWED_TRANSITIONS[current]


class IllegalTransition(Exception):
    """A *local* caller (not a provider) asked for a move the machine forbids: a bug."""


def assert_agent_transition(current: AgentState, target: AgentState) -> None:
    if not can_agent_transition(current, target):
        raise IllegalTransition(f"agent {current} -> {target} is not allowed")


# --------------------------------------------------------------------------------------
# Calls
# --------------------------------------------------------------------------------------

# Rank = "how far along the call is". Terminal states share the top rank.
CALL_RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}

TERMINAL_CALL_STATES = frozenset({
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
})
NON_TERMINAL_CALL_STATES = frozenset(CallState) - TERMINAL_CALL_STATES

CALL_ALLOWED_TRANSITIONS: dict[CallState, frozenset[CallState]] = {
    # Predictive calls have no agent yet, so QUEUED -> INITIATED skips RESERVED.
    CallState.QUEUED: frozenset({CallState.RESERVED, CallState.INITIATED, CallState.FAILED, CallState.CANCELLED}),
    CallState.RESERVED: frozenset({CallState.INITIATED, CallState.FAILED, CallState.CANCELLED}),
    # INITIATED -> ANSWERED/COMPLETED are the "RINGING event was lost or arrived late" paths.
    CallState.INITIATED: frozenset({CallState.RINGING, CallState.ANSWERED, CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}),
    CallState.RINGING: frozenset({CallState.ANSWERED, CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}),
    # ANSWERED is where predictive calls race for an agent: bridge (CONNECTED) or abandon
    # (FAILED, end_reason=ABANDONED). Progressive calls pass through it instantly.
    CallState.ANSWERED: frozenset({CallState.CONNECTED, CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}),
    CallState.CONNECTED: frozenset({CallState.COMPLETED, CallState.FAILED}),
    CallState.COMPLETED: frozenset(),
    CallState.FAILED: frozenset(),
    CallState.CANCELLED: frozenset(),
}


class CallDecision(enum.StrEnum):
    APPLY = "APPLY"  # legal forward move: perform it
    DUPLICATE = "DUPLICATE"  # target == current: already there, no-op
    STALE = "STALE"  # target is behind us (or we are terminal): record fact, change nothing
    ILLEGAL = "ILLEGAL"  # forward in rank but not an edge (e.g. QUEUED -> CONNECTED)


def decide_call_transition(current: CallState, target: CallState) -> CallDecision:
    """Classify a *requested* call move. Used for provider events, which we don't control.

    Order matters: terminal check first so that FAILED -> COMPLETED (same rank, different
    state) is STALE, not "ILLEGAL"; the call is finished and later events are just facts.
    """
    if target == current:
        return CallDecision.DUPLICATE
    if current in TERMINAL_CALL_STATES:
        return CallDecision.STALE
    if CALL_RANK[target] <= CALL_RANK[current]:
        return CallDecision.STALE
    if target in CALL_ALLOWED_TRANSITIONS[current]:
        return CallDecision.APPLY
    return CallDecision.ILLEGAL


def assert_call_transition(current: CallState, target: CallState) -> None:
    """For *local* callers (our own code), where anything but APPLY is a bug."""
    if decide_call_transition(current, target) is not CallDecision.APPLY:
        raise IllegalTransition(f"call {current} -> {target} is not allowed")


# --------------------------------------------------------------------------------------
# Mermaid rendering (docs/state-machines.md is checked against this by a test)
# --------------------------------------------------------------------------------------

def mermaid_edges(transitions: dict) -> list[str]:
    return [f"    {src} --> {dst}" for src, dsts in transitions.items() for dst in sorted(dsts)]


if __name__ == "__main__":
    print("stateDiagram-v2\n    [*] --> OFFLINE")
    print("\n".join(mermaid_edges(AGENT_ALLOWED_TRANSITIONS)))
    print()
    print("stateDiagram-v2\n    [*] --> QUEUED")
    print("\n".join(mermaid_edges(CALL_ALLOWED_TRANSITIONS)))
