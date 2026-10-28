"""Immutable snapshots of rows. A snapshot carries the `version` it was read at, which is
what makes compare-and-set natural: you hand the snapshot back to the repository to
change it, and the change only applies if the row is still at that version."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime

from smartdialer.states import AgentState, CallState


class Mode(enum.StrEnum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


class BorrowerStatus(enum.StrEnum):
    PENDING = "PENDING"  # may be called once next_attempt_at has passed
    RESERVED = "RESERVED"  # a call attempt is in flight
    DONE = "DONE"
    EXHAUSTED = "EXHAUSTED"  # max_attempts reached
    DO_NOT_CALL = "DO_NOT_CALL"


@dataclass(frozen=True, slots=True)
class Agent:
    id: int
    campaign_id: int
    name: str
    state: AgentState
    version: int
    state_since: datetime
    lease_until: datetime | None
    wrap_up_until: datetime | None
    pause_requested: bool = False

    @classmethod
    def from_record(cls, r) -> Agent:
        return cls(r["id"], r["campaign_id"], r["name"], AgentState(r["state"]), r["version"],
                   r["state_since"], r["lease_until"], r["wrap_up_until"], r["pause_requested"])


@dataclass(frozen=True, slots=True)
class Campaign:
    id: int
    name: str
    mode: Mode
    status: str
    max_abandon_rate: float
    wrap_up_seconds: int
    max_attempts: int

    @classmethod
    def from_record(cls, r) -> Campaign:
        return cls(r["id"], r["name"], Mode(r["mode"]), r["status"], float(r["max_abandon_rate"]),
                   r["wrap_up_seconds"], r["max_attempts"])


@dataclass(frozen=True, slots=True)
class Borrower:
    id: int
    campaign_id: int
    phone: str
    status: BorrowerStatus
    version: int
    priority: int
    attempts: int
    next_attempt_at: datetime
    last_outcome: str | None

    @classmethod
    def from_record(cls, r) -> Borrower:
        return cls(r["id"], r["campaign_id"], r["phone"], BorrowerStatus(r["status"]), r["version"],
                   r["priority"], r["attempts"], r["next_attempt_at"], r["last_outcome"])


@dataclass(frozen=True, slots=True)
class Call:
    id: int
    campaign_id: int
    borrower_id: int
    agent_id: int | None
    provider: str | None
    provider_call_id: str | None
    state: CallState
    version: int
    attempt_no: int
    lease_until: datetime | None
    end_reason: str | None
    created_at: datetime
    initiated_at: datetime | None
    ringing_at: datetime | None
    answered_at: datetime | None
    connected_at: datetime | None
    ended_at: datetime | None

    @classmethod
    def from_record(cls, r) -> Call:
        return cls(r["id"], r["campaign_id"], r["borrower_id"], r["agent_id"], r["provider"],
                   r["provider_call_id"], CallState(r["state"]), r["version"], r["attempt_no"],
                   r["lease_until"], r["end_reason"], r["created_at"], r["initiated_at"],
                   r["ringing_at"], r["answered_at"], r["connected_at"], r["ended_at"])
