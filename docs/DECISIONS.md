# Decisions

Format per entry: **Chose** · **Why** · **Makes harder**. Entries marked *(planned)* record
a decision made up-front that a later phase will implement; they are updated when built.

## D-01 PostgreSQL only: no cache, no Redis, no Kafka
- **Chose:** One PostgreSQL instance is the single source of truth for agents, borrowers,
  calls and provider events. Workers are stateless processes that read and write it.
- **Why:** Every hard requirement is a *consistency* requirement (no double-booked agent,
  one live call per borrower, exactly-once event application). A relational DB gives atomic
  multi-row transactions, row locks (`SKIP LOCKED`), unique constraints and partial indexes
  for free. A cache adds a second copy of "is this agent free?", which turns the interview
  question "DB says AVAILABLE, cache says RESERVED, who wins?" into a bug class. Here the
  answer is trivially "the DB; there is nothing else."
- **Makes harder:** All write throughput funnels through one primary. That is fine for
  100-1,000 agents and is the first thing to break at scale (see the scale section, Phase 8).

## D-02 Python asyncio + asyncpg, no ORM
- **Chose:** Hand-written SQL through `asyncpg`.
- **Why:** The interesting logic *is* the SQL (atomic reservation, CAS updates, dedup
  inserts). An ORM would hide the statements a reviewer needs to read.
- **Makes harder:** No migrations tooling; `schema.sql` is applied wholesale. Fine for a
  prototype, not for production.

## D-03 Explicit transition tables, call states have a rank
- **Chose:** `ALLOWED_TRANSITIONS` maps for agents and calls in `states.py`. Calls also
  have a numeric rank; a provider event may only move a call to a strictly higher rank.
- **Why:** Providers duplicate and reorder events. With ranks, "does this event apply?" is a
  pure function of (current state, event) so replaying any permutation of a normal call's
  events converges to the same final state (unit-tested exhaustively).
- **Makes harder:** A lost event can leave a call "behind" reality (we never saw RINGING, the
  call is actually connected). That is handled by skip edges (INITIATED → ANSWERED) and by
  the reconciler asking the provider for the truth, not by loosening the rules.

## D-04 Terminal states are absorbing; late events are facts, not transitions
- **Chose:** After COMPLETED/FAILED/CANCELLED nothing changes state. A late `ANSWERED` after
  `COMPLETED` is stored in `provider_events` (outcome `STALE`) and still feeds answer-rate
  statistics, but the call's state does not move.
- **Why:** Re-opening a finished call would double-release an agent or double-count it.
  The *fact* "this borrower answered" is still true and the pacer should learn from it.
- **Makes harder:** `calls.state` alone is not the whole story; stats must read
  `answered_at`/`provider_events`, not just state.

## D-05 Pause-after-wrap-up for busy agents
- **Chose:** There is no PAUSED edge from RESERVED/DIALING/CONNECTED. A break requested
  mid-call is remembered and applied when the agent reaches WRAP_UP.
- **Why:** Pausing a reserved agent would strand a call that already has a borrower.
- **Makes harder:** A break is not instantaneous; supervisors must understand the delay.

## D-06 Compare-and-set on a `version` column for every update
- **Chose:** `UPDATE … SET …, version = version + 1 WHERE id = $1 AND version = $2`; zero
  rows means we lost a race, so re-read and re-decide.
- **Why:** Optimistic concurrency is simple to reason about and never holds a lock across a
  provider call. Locks are used only inside the single reservation statement.
- **How:** Repositories take a *snapshot* (`Agent`, `Call`, `Borrower`) and write
  `WHERE id = $1 AND version = $2 AND state = $3`; `None` back means "lost the race".
  Two deliberate exceptions, both guarded by `state` and still bumping `version`: the atomic
  reservation statements (which *are* the race arbiter) and the set-based
  `end_expired_wrap_ups`.
- **Makes harder:** Every writer needs a retry/give-up path; hot rows could starve (not
  expected here: each agent/call row has few writers).

## D-07 The clock is injected, never `now()` in SQL
- **Chose:** Timestamps are parameters supplied by the application's `Clock`. Time columns
  have no `DEFAULT now()`.
- **Why:** The simulator needs a virtual clock and tests need determinism; leases and
  backoff are time-based, so SQL `now()` would make them untestable without sleeping.
- **Makes harder:** In production all workers need a sane shared time source (NTP). Skew
  between workers shortens/lengthens leases; lease durations are set with margin for it.

## D-08 Reserve the longest-idle agent
- **Chose:** `ORDER BY state_since` among AVAILABLE agents, partial index
  `agents_available_idx` makes this an index scan.
- **Why:** Fair distribution of calls (and commissions/talk-time in a real call-centre).
- **Makes harder:** Ordering means all workers contend for the *same* first rows; `SKIP
  LOCKED` is what keeps that from serialising them.

## D-09 Leases + provider idempotency reference
- **Chose:** `lease_until` on RESERVED/DIALING agents and in-flight calls; our `calls.id` is
  sent to the provider as a client reference.
- **Why:** A crashed worker cannot clean up after itself, so ownership must expire. The
  reconciler asks `provider.get_call_status()` rather than guessing. The client reference
  lets it find a call even if the crash happened before `provider_call_id` was saved.
- **Makes harder:** Lease length is a trade-off: short = fast recovery but false expiries on
  slow providers; long = slow recovery.

## D-10 Retry backoff lives on the borrower row
- **Chose:** `borrowers.next_attempt_at` + `attempts`; selection only considers PENDING
  borrowers whose time has come.
- **Why:** Retries need no separate queue; "when may this borrower be called again" is data
  next to the borrower, claimed with the same `SKIP LOCKED` pattern.

## D-11 Abandon is FAILED + `end_reason`, not a new state
- **Chose:** `FAILED` with `end_reason='ABANDONED'` and `answered_at` set.
- **Why:** The brief's state list has no abandon state and every terminal state should be
  interchangeable for the machine. The compliance number (abandoned ÷ answered) is a query.
- **Makes harder:** Anyone counting FAILED as "provider problems" must filter by reason.

## D-12 Every Safety Controller verdict is persisted with its inputs
- **Chose:** `safety_decisions` rows with verdict, proposed/approved n, reason, input numbers.
- **Why:** "Why did we dial 17, not 10?" must be answerable after the fact, and compliance
  review needs the audit trail.
- **Makes harder:** One insert per pacing tick; batched/sampled at high scale.

## D-13 DB constraints are a second line of defence, not the first
- **Chose:** Partial unique indexes (one live call per borrower and per agent), CHECK
  constraints on states, a CHECK that only RESERVED/DIALING agents carry a lease, and a
  bound on `safety_decisions`. Tests assert each one fires.
- **Why:** Application code will have bugs; the schema turns the worst ones (double booking)
  into a loud error instead of silent corruption.

## D-14 Provider events: one transaction, call row lock, unattachable events are not stored
- **Chose:** `apply_provider_event` runs in one transaction: lock the call row
  (`FOR UPDATE`), insert the event (`ON CONFLICT DO NOTHING`; a conflict bumps
  `duplicate_count` and stops), decide with `decide_call_transition`, CAS the call, record the
  outcome, then run an `on_applied` hook (agent release etc.) inside the same transaction.
- **Why:** The call lock serialises twin events for one call without slowing other calls.
  Event row + state change + side effects commit together, so a worker crash leaves either
  all or nothing, and the provider's redelivery then simply redoes it (tested by making the
  hook raise). Events resolve by `(provider, provider_call_id)`, falling back to the
  `client_ref` (our call id echoed back), which covers an event that beats `place_call()`'s
  response and a crash before `provider_call_id` was saved.
- **Makes harder:** An event we cannot attach to any call (`UNKNOWN_CALL`) is returned to the
  caller and *not* stored: storing it would let the dedup key swallow the one redelivery that
  could be attached. The cost is that such orphans are only visible in logs/metrics, and the
  reconciler (Phase 5) is what heals a call whose events never resolve.

## D-15 Progressive reservation is one transaction: agent + borrower + call row
- **Chose:** `reserve_progressive_call` claims an agent, a borrower and inserts the call
  (state RESERVED, same lease on both) atomically; no borrower available rolls the agent back.
- **Why:** There is no instant where an agent is RESERVED without a call, so a crash between
  steps cannot strand one. Both claims are `SKIP LOCKED`, so workers never wait on each other
  (no lock-order deadlock). Proven by tests: 50 workers / 10 agents x10 rounds, borrower
  contention, and a negative control showing the naive SELECT-then-UPDATE double-books; with
  the locking removed the race tests fail (checked by hand-mutation).
- **Makes harder:** When borrowers run out, workers briefly claim and roll back agents
  (wasted work, never wrong state). Phase 4's loop checks for dialable borrowers cheaply first.
  The *provider call* is still outside the transaction, so a crash after place_call needs the
  lease + reconciler (D-09): a transaction cannot span an external system.

## D-16 Circuit breaker: gates and counts place_call only; OPEN never blocks progressive dialing
- **Chose:** A rolling-window breaker (CLOSED / OPEN / HALF_OPEN; trips at >=50% failures over
  >=10 samples in 30s; OPEN fails fast; probes after 15s, wait doubles on a failed probe up to
  120s). `ProtectedProvider` wraps any provider; the dialer only ever holds the wrapper.
  `health()` is the signal the Safety Controller reads (Phase 6).
- **Why:** In an outage each `place_call` costs a full timeout (5-10s) *while holding an
  agent*. Failing fast frees agents immediately and stops piling load on a struggling
  provider. A ratio over a minimum sample size avoids tripping on two unlucky calls; the time
  window forgets old problems. Test: after tripping, 50 further calls fail in zero virtual time
  and the provider's attempt counter does not move.
- **Why only `place_call`:** `hangup` and `get_call_status` are how we *clean up* after an
  outage (reconciler, releasing calls); they must keep working while the breaker is open.
  `ProviderRejected` (bad number) counts as a success: the provider answered, the request was bad.
- **Makes harder:** OPEN must not stop dialing *forever* or nothing would probe and the
  breaker could never close. Implemented in the Safety Controller (Phase 4): while OPEN and
  before `retry_at` it REJECTs (no agent is reserved, so agents are not churned through
  reserve/fail cycles); after the cool-down, and while HALF_OPEN, it approves a single probe
  call per tick. A provider failure never burns a borrower's attempt (D-20).

## D-17 Virtual-time scheduler instead of real sleeps (and instead of mocking `asyncio.sleep`)
- **Chose:** `Scheduler`: tasks started with `spawn()` park on timers via `sleep()`; when no
  tracked task is runnable it jumps the `VirtualClock` to the earliest timer. Ties run in
  insertion order, so a run is reproducible; mock providers use a seeded `random.Random`.
- **Why:** Provider latencies, 10s timeouts, 30s lease expiries and 3-minute calls must be
  testable in milliseconds, and the simulator must replay a scenario bit-for-bit. Because
  real DB I/O inside a tracked task just keeps it "runnable", the same scheduler will drive the
  real-PostgreSQL simulation in Phase 7.
- **Makes harder:** Code under the scheduler must `sleep()` through it and use
  `Scheduler.gather` (not `asyncio.gather`); misuse raises or deadlocks loudly rather than
  silently running in real time. Two real bugs found while writing it (a parent waiting in
  `gather` was counted as runnable; the last child must hand its "runnable token" to the
  parent atomically) are fixed and covered by tests.

## D-18 Provider contract: idempotent `place_call`, ambiguous timeouts, lookup by `client_ref`
- **Chose:** `place_call` is idempotent on `client_ref` (= our `calls.id`); `get_call_status`
  accepts either id; every event echoes `client_ref`. A timeout is modelled as *ambiguous*
  (ProviderB places the call anyway about half the time).
- **Why:** After a timeout we cannot know whether the borrower's phone is ringing. Retrying
  blindly could call them twice (a compliance problem); never retrying could strand an agent.
  With an idempotent call and a status lookup by our own id, "look, then retry" is always safe
  and the reconciler (Phase 5) never has to guess.
- **Makes harder:** The contract is something a real provider has to support (Plivo-style
  custom reference / request UUID). Without it we would need to hold the borrower in an
  "unknown" state until the provider's own reporting caught up.
- **Mock outage semantics:** during `start_outage()` API calls time out and event callbacks are
  *lost* (not queued), while calls keep progressing inside the provider; after `end_outage()`
  `get_call_status` reports the truth. That is the hard case for the reconciler.

## D-19 Write-ahead dialing: record the intent (INITIATED/DIALING) BEFORE calling the provider
- **Chose:** Reserve (agent, borrower, call = RESERVED) -> commit. Then commit call
  `INITIATED` + agent `DIALING` -> *then* `place_call` -> then record the provider id.
- **Why:** A DB transaction cannot span an external system, so something is always in doubt
  during the provider call. Writing the intent first makes the doubt *visible and findable*:
  a crash anywhere after step 2 leaves an INITIATED call with a lease; the reconciler asks the
  provider about our `client_ref` (idempotent contract, D-18) instead of guessing. It also
  means no provider event can ever arrive for a call still in RESERVED.
- **Makes harder:** A crash between step 2 and `place_call` leaves a call that never existed at
  the provider; the reconciler must treat NOT_FOUND as "safe to fail and retry". The worst case
  is bounded by the lease, not by luck.
- **Visible limitation (Phase 5 fixes it):** if the provider is fully down, a timeout *and* the
  follow-up status check both fail. We then deliberately do nothing: the call stays INITIATED,
  the agent DIALING, both with a lease. `test_unresolvable_timeout_is_left_for_the_reconciler`
  pins this behaviour.

## D-20 Provider/agent faults refund the borrower's attempt
- **Chose:** `end_reason` decides what a failed call costs the borrower. NO_ANSWER, BUSY,
  ABANDONED, AGENT_DROPPED count as an attempt (exponential backoff). PROVIDER_ERROR,
  PROVIDER_TIMEOUT, CIRCUIT_OPEN, AGENT_LOST refund it and retry after ~10s. REJECTED (bad
  number) exhausts the borrower immediately. One function, `borrower_args`, holds the table.
- **Why:** Otherwise an outage or a wave of agents logging off silently burns through every
  borrower's `max_attempts` and the campaign ends with people nobody ever actually called.
- **Makes harder:** A borrower whose number *causes* provider errors could retry for a long
  time; mitigated by the breaker and, in production, by an overall retry budget per borrower.

## D-21 The Safety Controller issues permits it remembers; the allocator can only redeem them
- **Chose:** `review()` re-reads live counts, decides APPROVE / REDUCE / REJECT /
  FALLBACK_PROGRESSIVE, logs it, and mints a `Permit` (n, expiry, campaign). The allocator
  must call `safety.redeem(permit)` once per call. The controller keeps the issued objects in
  a private registry and checks *identity*, so a hand-built, copied, foreign, spent or expired
  permit does nothing. `Permit.__init__` also demands a private module token.
- **Why:** Python cannot make a class truly unconstructible, so the guarantee comes from
  state the attacker does not hold, not from naming conventions. There is no flag, config or
  mode that skips `review()`. Tests forge a permit with `object.__new__` and assert zero calls.
- **Two independent counts:** progressive approval is `min(proposed, AVAILABLE agents,
  online-agents minus agents bound to live calls)`. They are computed from different tables,
  so a bookkeeping bug can only reduce dialing.
- **Layered, not redundant:** even a permit for 50 cannot over-dial, because
  `reserve_progressive_call` can only succeed while a free agent exists. The permit is policy;
  the reservation is physics.
- **Makes harder:** Permits live in one process's memory (they are seconds-long, never
  persisted). That is correct for a permit, but it means safety state that must be shared
  across workers (e.g. the rolling abandon budget in Phase 6) has to come from PostgreSQL, not
  from the controller instance.

## D-22 One lock order everywhere: call -> agent -> borrower
- **Chose:** Every transaction that touches more than one of these locks them in that order.
  `apply_provider_event` already holds the call lock when its hook runs, so everything else
  conforms: `_intend`, `release_after_call`, and `agent_offline` (which learns the call from
  the agent, so it peeks unlocked, locks call then agent, re-verifies, and retries if a call
  appeared in between). The reservation path takes agent then borrower but only via
  `SKIP LOCKED`, which never waits.
- **Why:** Two workers locking the same pair in opposite orders is the classic deadlock;
  PostgreSQL would abort one of them mid-event. A single documented order removes the class.
- **Makes harder:** New code has to follow the rule. It is written down at the top of
  `completion.py` and exercised by the 3-worker and chaos tests.

## D-23 What "the agent disappeared" costs depends on how far the call got
- **Chose:** `agent_offline()` ends the agent's call by stage: before the borrower answered ->
  CANCELLED/AGENT_LOST, provider told to hang up, borrower refunded (**not** an abandon);
  answered but not bridged -> FAILED/ABANDONED (**is** an abandon, counted); mid-conversation ->
  FAILED/AGENT_DROPPED. If `place_call` is still in flight when the agent vanishes, the
  allocator hangs the call up as soon as the provider returns (compensating hangup).
- **Why:** Compliance counts only borrowers who answered into silence. Calls we cancel while
  still ringing hurt nobody, and the dialer must be free to do that quickly.
- **Reaction time:** one database transaction after detection; the provider hangup runs in the
  background and does not delay it. (Detection itself, e.g. a heartbeat timeout, is outside
  the prototype.)
- **Makes harder:** A race where the provider answers after we cancelled is possible; the
  borrower hears a call that is hung up immediately. Unavoidable without provider-side
  atomic "cancel if not answered", and logged as a stale ANSWERED fact.

## D-24 The reconciler has no private way to change state: it feeds truth through the event path
- **Chose:** For a call whose lease expired, ask the provider (`get_call_status`, by our
  `client_ref` if we never saved its id), translate the answer into the events we missed
  (RINGING / ANSWERED / COMPLETED / FAILED, with `seq = -1` meaning "observed by the
  reconciler"), and hand them to the same `EventProcessor.handle` that real callbacks use. Calls
  that never reached the provider (QUEUED/RESERVED) are failed as `WORKER_CRASH` without asking
  anyone, because `INITIATED` is committed before `place_call` (D-19).
- **Why:** One implementation of "what does ANSWERED do to the agent and the borrower?". A
  reconciler with its own copy would drift from the live path and fail in exactly the
  situations nobody tests. Rank rules make it monotonic (it can only move a call forward), and
  `seq = -1` makes re-observing the same fact a deduplicated no-op: running it twice, or two at
  once, cannot double-release an agent or double-refund a borrower (tested without the lock).
- **COMPLETED seen with no ANSWERED:** replayed as ANSWERED then COMPLETED, so the agent who
  really did talk is bridged, wrapped up and credited, and `answered_at` is on record.
- **Makes harder:** The reconciler's view is only as fresh as the provider's status endpoint,
  and a call we cannot attribute to an id relies on the provider honouring `client_ref` lookup.

## D-25 Never guess: an unreachable provider leaves calls (and agents) held, deliberately
- **Chose:** If `get_call_status` fails, change nothing and look again after
  `reconcile_retry_seconds`. The only conclusion drawn from absence is the provider's own
  `NOT_FOUND`, and even that is applied only if the call's `version` is unchanged since we looked.
- **Why:** The two wrong guesses are both bad. "It failed" while the borrower's phone is ringing
  frees an agent into a double-dial or an abandon; "it is fine" strands an agent forever.
  Waiting costs capacity, never correctness.
- **Makes harder (the honest trade-off):** During a long provider outage the agents holding
  in-flight calls stay occupied until the provider comes back, and that is lost utilisation.
  An operator lever ("force-release calls older than X") would be a policy decision with
  compliance consequences, so it is intentionally not automatic. The number of calls past lease
  that cannot be resolved is the alert to watch.

## D-26 `connected_lease_seconds` is the price of a lost end-of-call event
- **Chose:** 180s. A CONNECTED call's lease just says "check on this call by then".
- **Why:** If COMPLETED is lost, the agent sits in CONNECTED until the lease expires and the
  reconciler learns the call ended. So the lease *is* the worst-case wasted agent time per lost
  event, while the load it creates is about `connected_agents / lease` status queries per
  second. At 10,000 agents and 180s that is roughly 55 queries/s, fine; at 10s it would be 1,000/s.
- **Makes harder:** Two knobs pull in opposite directions, so the value is a business call.

## D-27 A slow worker is a zombie, and zombies are handled, not prevented
- **Chose:** The lease cannot tell "dead" from "very slow". If a worker's `place_call` outlives
  its lease the reconciler fails the call (provider says NOT_FOUND), frees the agent and requeues
  the borrower; when the zombie finally returns, `_record` sees a terminal call and hangs up the
  call it just created.
- **Why:** Preventing zombies needs fencing tokens through the provider, which we do not control.
  Detecting them and compensating is achievable and tested.
- **Makes harder:** There is a window (<= the zombie's remaining latency) where a borrower's
  phone may ring for a call nobody owns. The mitigation is to keep leases comfortably above the
  provider's worst-case latency (60s vs a 10s timeout), which is what `lease_seconds` is for.

## D-28 Advisory lock for efficiency, never for correctness
- **Chose:** `pg_try_advisory_lock(namespace, campaign_id)` on a dedicated pooled connection around
  each pass. Session-scoped: if the process dies, PostgreSQL releases it with the connection (tested).
- **Why:** With N workers each running the loop, you want one pass per interval, not N provider
  query storms. But every action is already safe to race (D-24), so a lock bug or a stale lock
  can cost duplicate queries and nothing else. Anything that needed the lock for correctness would
  be fragile (a lock holder can pause for a minute and wake up wrong).
- **Makes harder:** `try`-lock means a skipped pass is simply skipped; the next interval catches up.

## D-29 Two answer-rate estimators on purpose: optimistic in the pacer, pessimistic in the controller
- **Chose:** The pacer uses a time-decayed mean (EWMA, half-life 2 min, with a 10-call prior at 30%),
  recomputed statelessly each tick from recent outcomes. The Safety Controller ignores it, reads its
  own outcome counts from PostgreSQL, and uses the **Wilson upper bound** (one-sided 99%) of the
  answer rate over a 5-minute window and over a 1-minute window, taking the larger.
- **Why:** The pacer's job is to be useful: a best guess that follows the world. The controller's job
  is to be safe: "how high could the rate plausibly be?". Wilson stays honest at tiny samples
  (0 answers in 5 calls is not "0% +/- 0%") and returns 1.0 with no data, which makes the
  controller behave exactly like progressive until it has evidence. Taking the larger of two windows
  means a surge shows up in the short window immediately instead of after the long window catches up.
  Statelessness means a restarted worker, or three workers, compute identical numbers.
- **Makes harder:** The two numbers legitimately disagree (the pacer says 50%, the controller uses
  ~60%); the log shows both so the gap is explainable. Both read the `calls` table with
  window scans, which is fine at prototype scale and one of the first things to rework at 10k agents
  (see the scale section: incremental counters).

## D-30 "Safe" means P(more answers than free agents) <= 1%, found with a binomial tail
- **Chose:** With `m` calls outstanding and pessimistic rate `q`, answers ~ Binomial(m, q). Allow the
  largest `m` with `P(X > F) <= 1%`, where `F = AVAILABLE agents + agents whose wrap-up ends before
  even the fastest calls (10th-percentile setup time, clamped to 2-15 s) could be answered`. Allowed new
  calls = `min(proposal, m - already ringing)`. `m >= F` always (that is progressive), and `m` is
  capped at 4x `F` whatever the statistics say.
- **What it implies (tested):** small teams get nothing. With 5 free agents at a 50% rate even one
  extra call has a 1.6% chance of overflow, so `m = 5`. 20 agents may have 28 ringing (at q = 50%), 100 agents
  ~170. Pooling is what makes predictive dialing work, so the benefit grows with team size.
- **Deliberately conservative:** it treats all ringing calls as answering at once against the agents
  free *now*. In reality answers are spread over ~20 s and agents keep freeing up, so real abandon
  risk is far below 1%. I chose to be wrong in that direction. The honest price is utilisation: it
  does not use CONNECTED agents about to finish as capacity.
- **Makes harder:** the independence assumption (borrowers answer independently) fails for correlated
  events, such as everyone's phones being answered at lunchtime, which is why the shock detector and the
  abandon budget exist as separate backstops rather than trusting this one number.

## D-31 When predictive is not allowed: three triggers, all stateless
- **Chose:** Fall back to progressive sizing (FALLBACK_PROGRESSIVE; each call reserves its agent first)
  if ANY of: (1) circuit breaker not CLOSED; (2) **answer-rate shock**: the last 60 s differ from the data
  before them by more than max(15 points, 3 sigma), in either direction; (3) **abandon budget near its
  limit**: *one more abandon* in the 15-minute window would put the rate above 80% of the campaign's limit.
- **Why (3) is phrased that way:** `(abandoned + 1) / max(answered, 1) > 0.8 x limit` is true with no data,
  so a new campaign starts progressive and *earns* predictive dialing by accumulating about 42 answered
  calls at a 3% limit (about 84 if it has already had an abandon). No special-case "warm-up" code.
- **Shock is direction-agnostic:** a surge breaks our over-dial assumptions (abandons); a collapse means
  the model is wrong and we are mis-pacing agents. Either way we stop trusting it until the data settles.
  The brief's "predicted 70%, dropped to 10%": the short window flips the shock test within about 15
  resolved calls, the controller falls back, and the pacer's EWMA (half-life 2 min) catches up afterwards.
  (Tested with exactly those numbers.)
- **Makes harder:** after any shock the campaign is progressive until the windows re-converge (up to
  ~5 minutes). Scenario D in Phase 7 measures the cost of that safety.

## D-32 Predictive: the agent is chosen at the instant of the answer, by the database
- **Chose:** Predictive calls are inserted agentless (`QUEUED -> INITIATED`, no agent held). When the
  ANSWERED event is processed (inside the event transaction) `reserve_agent` runs: the same single
  `UPDATE ... FOR UPDATE SKIP LOCKED` used by progressive, longest-idle first. Got one: the call becomes
  CONNECTED with `agent_id` set and the agent CONNECTED, atomically. Got none: the call is FAILED with
  `end_reason = 'ABANDONED'`, `answered_at` kept, the borrower is retried with backoff (it counts as an
  attempt, they did answer), and the provider is told to hang up after commit.
- **Why:** Nothing earlier in the pipeline can promise an agent, so claiming that promise anywhere else
  would be a lie that fails under concurrency. Tested with 20 simultaneous answers against 8 agents: exactly
  8 distinct agents bound, exactly 12 abandons recorded, no agent shared.
- **Makes harder:** the abandon is a normal, expected, *counted* outcome of this mode. The safety
  system's job is to keep it rare and prove it, not to pretend it cannot happen.

## D-33 Excess ringing calls are hung up, newest first, and it is not an abandon *(thresholds and verification refined in D-39 to D-41)*
- **Chose:** The controller is consulted every tick for predictive campaigns, even when the pacer proposes
  nothing. If calls ringing exceed the safe level by more than max(2, 20%), it orders the excess cancelled
  (`Decision.hangup_n`): CANCELLED / `HUNG_UP_EXCESS`, borrower attempt refunded and retried in ~10 s,
  provider hangup in the background. In fallback mode the target is "every ringing call has an agent".
- **Why newest first:** least rung, so the borrower's experience is closest to a missed call. Not an
  abandon because nobody answered, so nobody was left waiting; it is excluded from answer-rate statistics.
- **Hysteresis:** every answer consumes a free agent, so the limit dips by about one per answer while ringing
  falls by one. Hanging up on that noise would make ghost-ring churn the norm. Small overshoots just stop
  new dialing; only material excess (agents dropped, a surge) is cut.
- **100 agents -> 60 (tested):** after 40 agents vanish, the next tick (no virtual time passes: the reaction
  is one DB transaction) hangs up exactly the excess, ringing returns to the safe level for 60 agents, and
  there are no abandons.
- **Makes harder:** a call answered in the instant before we cancel it arrives as a stale ANSWERED fact
  and cannot reopen the call; that borrower hears a dropped call. Closing this window needs a provider
  that can atomically "cancel if not answered".

## D-34 Controller authority rules that keep the pacer powerless
- **Chose:** The campaign's mode is read from the database (a pacer cannot opt itself into predictive).
  Approved is never more than proposed (a database CHECK enforces it), so the controller only ever reduces,
  never inflates. A fallback permit is a PROGRESSIVE permit: the agent is reserved before dialing.
  Fallback sizing subtracts agentless calls already ringing, so a fallback can never spend the agents that
  predictive calls in flight are counting on.
- **Why:** These are the properties an adversarial reviewer would test first; each has a test.

## D-35 What the numbers say so far (and what I would change)
- **Measured (60 agents, 40% answer rate, 60 s talk, 600 s, same seed):** progressive utilisation 44.6%
  (685 calls, 288 answered, 0 abandons); predictive **51.6%** (808 calls, 338 answered, 0 abandons). About
  110 of the 600 seconds were spent in fallback while the controller gathered its first evidence.
- **Reading it honestly:** a ~16% relative gain with zero abandons at 1% tail risk is a conservative
  operating point. The knobs that trade safety margin for utilisation are exposed in `SafetyConfig`
  (`epsilon`, windows, credit horizon, `max_ratio`) and the abandon budget is the campaign's own number.
  Phase 7 runs Scenarios A-D over a longer horizon with both providers and reports what moves.
- **What I would add with another week:** count CONNECTED agents expected to finish within the setup time
  as capacity (needs a talk-time model); per-agent-skill pools; and make the shock detector a proper
  CUSUM instead of a two-window comparison.

## D-36 The simulator runs the REAL dialer on a virtual clock; only the outside world is simulated
- **Chose:** Time, borrowers (answer rate, talk time), the telecom providers and incidents (outage, agents
  vanishing) are simulated. The pacer, Safety Controller, allocator, event handler and reconciler are the
  production code, running real SQL against PostgreSQL. One virtual second = one worker tick + every timer
  due in that second + a reconciler pass every 5 s. No sleeping anywhere: a 15-minute campaign takes
  seconds to a minute of wall time.
- **Why:** A simulator of a model of the dialer proves things about the model. This one is evidence about
  the system: the same code paths the tests exercise produce the report.
- **Makes harder:** It needs PostgreSQL (each run gets its own throwaway `*_sim` database; `reset_schema`
  refuses any other name) and costs ~50 ms of wall time per virtual second per 100 agents. The CLI runs
  scenarios in parallel worker processes, each with its own database.

## D-37 Reproducibility needed more than a seed: random draws are keyed by call, not drawn from one stream
- **Problem found:** with one shared `random.Random`, the order of draws depended on which concurrent task
  reached the provider first, and with real database I/O in the loop that order differs from run to run.
  Same seed, different results.
- **Chose:** Every draw in the mock providers is keyed by what it is about (`seed | place | call id | attempt`,
  `seed | script | call id`, `seed | emit | provider call id | seq`), and the provider's call id is derived from
  our call id instead of an arrival-order counter. Results no longer depend on task order.
- **Evidence:** `test_same_seed_same_run_exactly` runs the whole campaign twice and compares every
  number (`==`), with provider B's duplicates and reordering enabled.
- **Makes harder:** random draws cannot be cheaply "continued"; each use builds a small `Random`. At this
  scale that costs nothing measurable.

## D-38 Load test: what it measures, and what it honestly cannot tell you
- **Chose:** `python -m smartdialer.loadtest` runs in REAL time (it is measuring the database). 50 workers
  loop reserve (agent + borrower + call, one transaction) then free it, at 100 / 1,000 / 10,000 agents; it
  reports reservations/s, p50/p95/p99/max latency, the per-tick cost of `load_snapshot`, and checks for double
  booking two ways (an in-memory held-agent set and database invariants). Result on the development
  machine (Docker Desktop on Windows, one Python client): ~710 reservations/s, p99 56-63 ms, **flat from
  100 to 10,000 agents**, zero violations.
- **Reading it:** the flat line is the real finding: the reservation hits a partial index
  (`agents_available_idx`) and `SKIP LOCKED` keeps workers off each other, so cost does not grow with agents.
  What does grow is the per-tick snapshot (5.1 ms -> 5.7 ms -> 7.8 ms at 100 / 1k / 10k agents), because it
  aggregates the agents table.
- **Honest limits:** throughput plateaus because of the *client*: one Python process and Docker-on-Windows
  commit latency, not PostgreSQL's ceiling. Running two generators against separate databases reached ~1,000/s
  combined (about 1.2x one generator), so the local server is also near its limit. Latency grows with worker
  count (Little's law: 5 workers 6 ms, 20 workers 12 ms, 50 workers 34 ms at the median), as expected of a
  saturated resource. Treat the absolute numbers as a floor for this laptop, and the shape (flat in agents,
  linear in concurrency) as the result.

## D-39 Answer rates are measured by DIAL COHORT, not by resolution time (a bug the simulator found)
- **Symptom:** in Scenario A with the slow provider, predictive was barely better than progressive (34% -> 36%),
  spent most of the run in fallback, and its rolling abandon rate peaked at 4.8% (budget 3%).
- **Cause:** I counted outcomes as they *resolved*. A call that will be answered resolves after 6-24 s; one that
  will not rings out its 30 s timeout (plus callback delay). So right after a burst of dialing the recent window
  held the fast answers and none of the slow no-answers: an artificial ~100% answer rate. The shock detector took
  it for a surge and fell back; the fallback cut 151 ringing calls at once; 3 of them were dead-air hangups
  (D-40), which on ~60 answered calls is 4.8%, which kept the abandon budget exhausted for the rest of the window.
  A cascade from one statistical bias.
- **Chose:** a call enters the statistics by when it was DIALED and only once its whole cohort has had
  `MATURITY_S` = 35 s (30 s ring timeout + margin) to resolve. Fast and slow outcomes then count equally.
  Used by the pacer's moving average, the Wilson bounds and the shock detector.
- **Result (Scenario A, 20% answer rate):** provider B 36.0% -> 58.2% utilisation, abandon peak 4.84% -> 0.83%,
  hang-ups 145 -> 31; provider A 62.3% -> 63.2%, hang-ups 208 -> 20. `test_a_burst_of_fast_answers_is_not_mistaken_for_a_surge`
  pins it.
- **Makes harder:** the estimate lags a real change by ~35 s. The fast paths for that gap are the abandon budget
  and the answerable-rings logic (D-41), not the rate estimate.

## D-40 Never hang up on a call you have not verified; dead air is an abandon
- **Problem found:** with provider B (callbacks up to 15 s late) our database often says RINGING for a call the
  borrower has already answered. The controller cancelled 26 such calls in one run (about 9% of answered calls):
  dead air, which our records did not count as an abandon, so "0 abandons" overstated compliance.
- **Chose:** `hang_up_excess` only *picks* candidates. Each pick is verified with the provider (`get_call_status`
  by our `client_ref`): still ringing -> cancel; already answered/finished -> do NOT cancel, feed the truth through
  the normal event path (the borrower gets an agent, or an honest ABANDONED is recorded); provider unreachable ->
  do nothing (never guess, same rule as the reconciler, D-25). A call whose hangup is being verified is not picked
  again by the next tick. Provider->events translation is shared with the reconciler (`dialer/observed.py`).
- **Counting:** a call we cancelled for which the provider later reports ANSWERED counts as an abandon (and as
  answered) in the abandon budget and in every simulation report; it is shown in the "dead air" figure.
- **Residual risk (unavoidable):** between the verification reply and the hangup taking effect the borrower can
  still answer. Closing that needs an atomic "cancel if not answered" at the provider. Measured after all fixes:
  7 abandons in 5,134 answered predictive calls (0.14%), of which the dead-air ones are 7 (all provider B).

## D-41 Two thresholds: stop DIALING at 1% overflow risk, only HANG UP above 25%; stale rings do not count
- **Chose:** `epsilon = 1%` governs starting calls. `epsilon_hangup = 25%` governs cancelling them. Calls that have
  rung longer than the 99th-percentile time-to-answer actually observed (needs >= 50 samples; default 30 s) are
  excluded from the overflow maths and from the pacer's target: they are no-answers waiting out their timeout.
- **Why:** hang-ups have their own harm (ghost rings, and the D-40 race). A count-based tolerance misfired: at a
  low answer rate the allowed outstanding is ~4x free agents, so each answer lowers the limit by ~4 while removing
  one ringing call, and healthy calls were cut (23% of all calls in Scenario A). A probability band makes normal
  churn harmless while a real agent drop (100 -> 60 agents, tested) is still cut immediately.
- **Honest attribution:** the cohort fix (D-39) did most of the work (Scenario A hang-ups 208 -> 59);
  moving the cut threshold from 5% to 25% removed about two thirds of the rest (59 -> 20) with unchanged
  utilisation and abandons. Fallback mode keeps a deterministic count-based cut ("every ringing call has an agent").
- **Makes harder:** between 1% and 25% risk we neither dial nor cut: the abandon budget (not this rule) is
  the backstop there. That is deliberate and is why the budget fallback exists.

## D-42 What the full simulation says (100 agents, 24 runs: `docs/SIMULATION_RESULTS.md`)
Steady-state utilisation (after the first 3 minutes), progressive -> predictive; abandons are effective
(recorded abandons + dead-air hang-ups) as a share of answered calls:

| Scenario | Provider A | Provider B | Abandons A / B |
|---|---|---|---|
| A: 20%, 120 s | 39.2 -> 63.2% (+61%) | 34.1 -> 58.2% (+71%) | 0 / 1 |
| B: 50%, 90 s | 54.4 -> 63.2% (+16%) | 48.4 -> 58.1% (+20%) | 0 / 0 |
| C: 70%, 180 s | 75.1 -> 79.2% (+5%) | 71.0 -> 76.0% (+7%) | 0 / 0 |
| D: changing | 52.1 -> 56.7% (+9%) | 46.3 -> 53.0% (+15%) | 0 / 0 |
| E: provider outage | 53.9 -> 62.0% (+15%) | 47.4 -> 57.1% (+20%) | 0 / 0 |
| F: 40% of agents vanish | 53.8 -> 62.1% (+15%) | 47.2 -> 54.3% (+15%) | 0 / 6 |

- **Safety:** 7 abandons in 5,134 answered predictive calls (0.14%); progressive 0. The worst run (F with the slow
  provider, 6 abandons) peaked at 2.53% rolling, under the 3% limit, and spent 151 of 694 ticks in
  abandon-budget fallback: the backstop doing its job.
- **Where it pays:** the gain tracks idle time. Low answer rates (A) leave progressive agents waiting most of the
  time, so predictive adds 24 points; at a 70% answer rate (C) agents are already busy and it adds 4-5. The
  slow provider B benefits most because predictive hides its dial latency.
- **Fallback is visible:** breaker-driven fallbacks appear in E (29-33 ticks), shock-driven in D (about 45% of
  ticks: the cost of a world that keeps changing), budget-driven in F. Every fallback trigger fires in the scenario built to provoke it.
- **Stress (`--epsilon 0.5`, overflow limit loosened 50x):** utilisation rises 3-4 points (B: 67.2% / 63.2%) with
  at most 4 abandons (0.75%) in D with the slow provider: the 1% rule is far more conservative than the 3% budget
  needs. That is the main tuning lever; I kept the default because "overflow risk <= 1%" is the guarantee I can defend.
- **Limits of this evidence:** one seed per run; borrowers answer independently (no correlation such as a time of
  day); mock providers; 100 agents. Larger teams would gain more (D-30). The numbers are reproducible exactly
  (D-37), so a reviewer can rerun them, and vary the seed.

## D-43 Where the scale answer comes from, and how it is kept honest
- **Chose:** the README's scale section separates what was *measured* (reservation throughput and latency flat
  from 100 to 10,000 agents; snapshot 5.1 to 7.8 ms) from what is *reasoned* (row growth, hot counter rows,
  status-lookup load, partitioning). It names one thing that breaks first: the per-tick aggregate reads
  (snapshot, cohort windows over `calls`), because reservation is O(1) by design and they are O(agents) and
  O(recent calls).
- **Why:** an answer of "add servers" does not say where the bottleneck is. The fix order follows cost: index the
  windows, partition `calls`/`provider_events`, then maintain counters in the transition transaction (shard them
  if the counter row gets hot), then shard by campaign for 100,000.
- **Makes harder:** incremental counters can drift from the truth, so they would need a periodic recount against
  the table (the same "database is the single source of truth" rule, D-01); a cross-campaign agent pool does
  not shard by campaign and is left as the open design question.
- **Docs are tested where they can be:** the README and `docs/state-machines.md` diagrams are compared to the
  transition tables by a unit test; numbers in docs come from `docs/sim_results.json` and `docs/load_results.json`.
