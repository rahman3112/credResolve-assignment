"""Connection helpers. Deliberately thin: no ORM, SQL stays visible in the repositories."""

from __future__ import annotations

import os
from importlib import resources

import asyncpg

DEFAULT_DSN = "postgresql://dialer:dialer@localhost:5432/dialer"
DEFAULT_TEST_DSN = "postgresql://dialer:dialer@localhost:5432/dialer_test"


def dsn() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DSN)


def test_dsn() -> str:
    return os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DSN)


def schema_sql() -> str:
    return resources.files("smartdialer").joinpath("schema.sql").read_text(encoding="utf-8")


async def create_pool(target: str | None = None, **kw) -> asyncpg.Pool:
    return await asyncpg.create_pool(target or dsn(), **kw)


async def reset_schema(conn: asyncpg.Connection) -> None:
    """DESTRUCTIVE: drop everything and re-apply schema.sql. Only ever used on dialer_test
    and by the simulator's own throwaway database."""
    name = await conn.fetchval("SELECT current_database()")
    if not name.endswith(("_test", "_sim")):
        raise RuntimeError(f"refusing to reset non-throwaway database {name!r}")
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await conn.execute(schema_sql())
