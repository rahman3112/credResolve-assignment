-- SmartDialer schema. PostgreSQL is the single source of truth (D-01).
--
-- Conventions
--   * Every mutable row has `version bigint`. All writes are compare-and-set:
--         UPDATE ... SET ..., version = version + 1 WHERE id = $1 AND version = $2
--     0 rows updated = someone else got there first = re-read and re-decide (D-06).
--   * No column defaults of now(): the application passes `now` explicitly so the
--     simulator can run on a virtual clock and tests are deterministic (D-07).
--   * State columns are text + a *named* CHECK constraint. tests/unit/test_schema_enums.py
--     compares each CHECK list to the Python enum so the two cannot drift apart.

CREATE TABLE campaigns (
    id                   bigserial PRIMARY KEY,
    name                 text        NOT NULL,
    mode                 text        NOT NULL,
    status               text        NOT NULL DEFAULT 'ACTIVE',
    -- Regulatory-style ceiling on abandoned / answered over the rolling window (3%).
    max_abandon_rate     numeric     NOT NULL DEFAULT 0.03,
    wrap_up_seconds      integer     NOT NULL DEFAULT 30,
    max_attempts         integer     NOT NULL DEFAULT 3,
    created_at           timestamptz NOT NULL,
    CONSTRAINT campaigns_mode_chk   CHECK (mode IN ('PROGRESSIVE', 'PREDICTIVE')),
    CONSTRAINT campaigns_status_chk CHECK (status IN ('ACTIVE', 'PAUSED', 'STOPPED'))
);

CREATE TABLE agents (
    id             bigserial PRIMARY KEY,
    campaign_id    bigint      NOT NULL REFERENCES campaigns (id),
    name           text        NOT NULL,
    state          text        NOT NULL DEFAULT 'OFFLINE',
    version        bigint      NOT NULL DEFAULT 0,
    -- When the agent entered `state`. For AVAILABLE agents this is "idle since", so
    -- ORDER BY state_since picks the longest-idle agent (fairness, D-08).
    state_since    timestamptz NOT NULL,
    -- Only set while RESERVED/DIALING. If a worker dies holding an agent, the reconciler
    -- finds it by this deadline (D-09). CONNECTED agents are covered by the call's lease.
    lease_until    timestamptz,
    wrap_up_until  timestamptz,
    -- A break requested while busy is remembered here and applied when wrap-up ends (D-05).
    pause_requested boolean    NOT NULL DEFAULT false,
    CONSTRAINT agents_state_chk CHECK (state IN
        ('OFFLINE', 'AVAILABLE', 'RESERVED', 'DIALING', 'CONNECTED', 'WRAP_UP', 'PAUSED')),
    CONSTRAINT agents_lease_chk CHECK (lease_until IS NULL OR state IN ('RESERVED', 'DIALING'))
);
-- The reservation query scans exactly this: AVAILABLE agents of a campaign, oldest first.
CREATE INDEX agents_available_idx ON agents (campaign_id, state_since, id) WHERE state = 'AVAILABLE';
CREATE INDEX agents_lease_idx     ON agents (lease_until) WHERE state IN ('RESERVED', 'DIALING');
CREATE INDEX agents_wrapup_idx    ON agents (wrap_up_until) WHERE state = 'WRAP_UP';

CREATE TABLE borrowers (
    id               bigserial PRIMARY KEY,
    campaign_id      bigint      NOT NULL REFERENCES campaigns (id),
    phone            text        NOT NULL,
    status           text        NOT NULL DEFAULT 'PENDING',
    version          bigint      NOT NULL DEFAULT 0,
    priority         integer     NOT NULL DEFAULT 0,
    attempts         integer     NOT NULL DEFAULT 0,
    -- Retry backoff lives here: a failed attempt pushes this into the future (D-10).
    next_attempt_at  timestamptz NOT NULL,
    last_outcome     text,
    CONSTRAINT borrowers_status_chk CHECK (status IN
        ('PENDING', 'RESERVED', 'DONE', 'EXHAUSTED', 'DO_NOT_CALL'))
);
CREATE INDEX borrowers_pick_idx ON borrowers (campaign_id, priority DESC, next_attempt_at, id)
    WHERE status = 'PENDING';

CREATE TABLE calls (
    id               bigserial PRIMARY KEY,
    campaign_id      bigint      NOT NULL REFERENCES campaigns (id),
    borrower_id      bigint      NOT NULL REFERENCES borrowers (id),
    -- NULL for a predictive call until it is ANSWERED and an agent is reserved for it.
    -- The call->agent link lives ONLY here (agents has no current_call_id) so there is
    -- one place to be wrong, not two.
    agent_id         bigint      REFERENCES agents (id),
    provider         text,
    -- Our call id is also sent to the provider as an idempotency/client reference, so a
    -- worker that crashed before saving provider_call_id can still be reconciled (D-09).
    provider_call_id text,
    state            text        NOT NULL DEFAULT 'QUEUED',
    version          bigint      NOT NULL DEFAULT 0,
    attempt_no       integer     NOT NULL,
    lease_until      timestamptz,
    -- Why a call ended: NO_ANSWER, BUSY, REJECTED (bad number), PROVIDER_ERROR,
    -- PROVIDER_TIMEOUT, CIRCUIT_OPEN, ABANDONED (answered, no agent: the compliance number),
    -- AGENT_LOST (agent vanished before the borrower answered; we cancelled),
    -- AGENT_DROPPED (agent vanished mid-conversation), HUNG_UP_EXCESS (predictive: we
    -- cancelled still-ringing calls; NOT an abandon), ...
    end_reason       text,
    created_at       timestamptz NOT NULL,
    initiated_at     timestamptz,
    ringing_at       timestamptz,
    answered_at      timestamptz,   -- stays set after abandon: "answered" is a fact (D-04)
    connected_at     timestamptz,
    ended_at         timestamptz,
    CONSTRAINT calls_state_chk CHECK (state IN
        ('QUEUED', 'RESERVED', 'INITIATED', 'RINGING', 'ANSWERED', 'CONNECTED',
         'COMPLETED', 'FAILED', 'CANCELLED')),
    CONSTRAINT calls_end_reason_chk CHECK
        (end_reason IS NULL OR state IN ('COMPLETED', 'FAILED', 'CANCELLED')),
    UNIQUE (provider, provider_call_id)
);

-- RULE 3: a borrower can have at most ONE non-terminal call. This is the backstop for
-- borrower allocation: even if two workers both believe they own a borrower, the second
-- INSERT fails. Terminal states are listed here and in states.py; test_schema_enums.py
-- fails if they disagree.
CREATE UNIQUE INDEX calls_one_active_per_borrower
    ON calls (borrower_id) WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');

-- Same idea for agents: an agent is bound to at most one live call. (NULL agent_id, i.e.
-- not-yet-bridged predictive calls, is excluded so any number of them may be ringing.)
CREATE UNIQUE INDEX calls_one_active_per_agent
    ON calls (agent_id) WHERE agent_id IS NOT NULL AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');

-- Reconciler: find expired in-flight calls without scanning terminal ones.
CREATE INDEX calls_lease_idx ON calls (lease_until) WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
-- Pacing and the safety controller count live calls per campaign and abandons over a window.
CREATE INDEX calls_campaign_active_idx ON calls (campaign_id, state) WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
CREATE INDEX calls_campaign_answered_idx ON calls (campaign_id, answered_at) WHERE answered_at IS NOT NULL;

-- RULE 5: every provider callback is stored once. Duplicates hit the UNIQUE constraint
-- and are counted, not re-applied. The row insert and the call state transition happen in
-- ONE transaction, so "event recorded but state not moved" (or the reverse) cannot exist.
CREATE TABLE provider_events (
    id                bigserial PRIMARY KEY,
    provider          text        NOT NULL,
    provider_call_id  text        NOT NULL,
    event_type        text        NOT NULL,
    -- Provider's per-call sequence number if it has one, else 0. Lets a provider that
    -- legitimately repeats an event type (e.g. two DTMF events) stay distinct.
    seq               integer     NOT NULL DEFAULT 0,
    -- NOT NULL: events we cannot attach to a call are not stored (see events.py).
    call_id           bigint      NOT NULL REFERENCES calls (id),
    occurred_at       timestamptz,          -- provider's clock; informational only
    received_at       timestamptz NOT NULL, -- our clock
    payload           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- What we did with it the first time we saw it.
    outcome           text        NOT NULL,
    duplicate_count   integer     NOT NULL DEFAULT 0,
    CONSTRAINT provider_events_outcome_chk CHECK (outcome IN
        ('APPLIED', 'DUPLICATE', 'STALE', 'ILLEGAL')),
    UNIQUE (provider, provider_call_id, event_type, seq)
);
CREATE INDEX provider_events_call_idx ON provider_events (call_id);

-- Every Safety Controller verdict, with the numbers that produced it, so "why did we dial
-- 17 and not 10?" is answerable after the fact (D-12).
CREATE TABLE safety_decisions (
    id           bigserial PRIMARY KEY,
    campaign_id  bigint      NOT NULL REFERENCES campaigns (id),
    decided_at   timestamptz NOT NULL,
    proposed_n   integer     NOT NULL,
    approved_n   integer     NOT NULL,
    verdict      text        NOT NULL,
    reason       text        NOT NULL,
    inputs       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT safety_decisions_verdict_chk CHECK (verdict IN
        ('APPROVE', 'REDUCE', 'REJECT', 'FALLBACK_PROGRESSIVE')),
    CONSTRAINT safety_decisions_bounds_chk CHECK (approved_n >= 0 AND approved_n <= proposed_n)
);
CREATE INDEX safety_decisions_campaign_idx ON safety_decisions (campaign_id, decided_at);
