from __future__ import annotations

from datetime import datetime, timedelta

from smartdialer.repo.models import Borrower, BorrowerStatus


async def create_borrower(conn, campaign_id: int, phone: str, now: datetime, *, priority: int = 0) -> Borrower:
    r = await conn.fetchrow(
        "INSERT INTO borrowers (campaign_id, phone, priority, next_attempt_at) "
        "VALUES ($1, $2, $3, $4) RETURNING *", campaign_id, phone, priority, now)
    return Borrower.from_record(r)


async def get_borrower(conn, borrower_id: int, *, for_update: bool = False) -> Borrower | None:
    r = await conn.fetchrow(f"SELECT * FROM borrowers WHERE id = $1{' FOR UPDATE' if for_update else ''}", borrower_id)
    return Borrower.from_record(r) if r else None


async def reserve_borrower(conn, campaign_id: int, now: datetime) -> Borrower | None:
    """Same atomic pattern as reserve_agent (RULE 2): highest priority, then the one
    that has waited longest. `attempts` is incremented here, at claim time, so a worker that
    crashes mid-call still counts the attempt and cannot retry a bad number forever.

    Backstop: even if this were wrong, calls_one_active_per_borrower would reject a second
    live call for the same borrower at INSERT time."""
    r = await conn.fetchrow(
        """
        UPDATE borrowers
           SET status = 'RESERVED', version = version + 1, attempts = attempts + 1
         WHERE id = (SELECT id FROM borrowers
                      WHERE campaign_id = $1 AND status = 'PENDING' AND next_attempt_at <= $2
                      ORDER BY priority DESC, next_attempt_at, id
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1)
           AND status = 'PENDING'
        RETURNING *
        """,
        campaign_id, now)
    return Borrower.from_record(r) if r else None


def backoff_seconds(attempts: int, base: float = 30.0, cap: float = 3600.0) -> float:
    """Exponential: 30s, 60s, 120s ... capped (D-10). `attempts` counts the attempt that
    just failed, so the first retry waits `base`."""
    return min(cap, base * 2 ** max(0, attempts - 1))


async def finish_borrower(conn, borrower: Borrower, now: datetime, *, done: bool, outcome: str,
                          max_attempts: int, base_backoff: float = 30.0,
                          refund_attempt: bool = False, retry_delay: float | None = None,
                          status: BorrowerStatus | None = None) -> Borrower | None:
    """Close out an attempt (CAS on the version we read).

    done=True              -> DONE
    status=...             -> forced terminal status (e.g. EXHAUSTED for a number the provider rejected)
    otherwise              -> back to PENDING after a delay, or EXHAUSTED once max_attempts is spent.
    refund_attempt=True    -> the attempt was not the borrower's fault (provider down, circuit
                              open, agent vanished before they answered): don't count it, so an
                              outage cannot burn through a borrower's retries.
    retry_delay            -> overrides exponential backoff (short delay for provider-side failures).
    Returns None if someone else changed the row first."""
    attempts = max(0, borrower.attempts - 1) if refund_attempt else borrower.attempts
    if status is not None:
        new_status, next_at = status, borrower.next_attempt_at
    elif done:
        new_status, next_at = BorrowerStatus.DONE, borrower.next_attempt_at
    elif attempts >= max_attempts:
        new_status, next_at = BorrowerStatus.EXHAUSTED, borrower.next_attempt_at
    else:
        new_status = BorrowerStatus.PENDING
        delay = retry_delay if retry_delay is not None else backoff_seconds(attempts, base_backoff)
        next_at = now + timedelta(seconds=delay)
    r = await conn.fetchrow(
        """
        UPDATE borrowers SET status = $4, next_attempt_at = $5, last_outcome = $6, attempts = $7,
                             version = version + 1
         WHERE id = $1 AND version = $2 AND status = $3
        RETURNING *
        """,
        borrower.id, borrower.version, borrower.status.value, new_status.value, next_at, outcome, attempts)
    return Borrower.from_record(r) if r else None
