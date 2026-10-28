from __future__ import annotations

import asyncpg
import pytest_asyncio
import pytest

from smartdialer import db


@pytest_asyncio.fixture
async def conn():
    """A connection to a freshly reset dialer_test database. Skips (not fails) when
    Postgres isn't up, so `pytest -m "not integration"` and a bare `pytest` both work."""
    try:
        c = await asyncpg.connect(db.test_dsn(), timeout=3)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"PostgreSQL not reachable ({e}); run `docker compose up -d --wait`")
    try:
        await db.reset_schema(c)
        yield c
    finally:
        await c.close()


@pytest_asyncio.fixture
async def pool():
    """Pool of 60 connections (> the 50 concurrent workers the race test uses) on a freshly
    reset dialer_test."""
    try:
        c = await asyncpg.connect(db.test_dsn(), timeout=3)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"PostgreSQL not reachable ({e}); run `docker compose up -d --wait`")
    try:
        await db.reset_schema(c)
    finally:
        await c.close()
    p = await asyncpg.create_pool(db.test_dsn(), min_size=1, max_size=60)
    try:
        yield p
    finally:
        await p.close()
