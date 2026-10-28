"""Throwaway databases for simulation runs. Names must end in `_sim` (db.reset_schema refuses
anything else), so a simulation can never wipe the real `dialer` database."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from smartdialer import db


def dsn_for(dbname: str) -> str:
    parts = urlsplit(db.dsn())
    return urlunsplit(parts._replace(path="/" + dbname))


async def ensure_sim_database(name: str) -> str:
    """Create `name` if missing and return its DSN. `name` must look like dialer_xxx_sim."""
    if not re.fullmatch(r"[a-z0-9_]+_sim", name):
        raise ValueError(f"simulation database names must match [a-z0-9_]+_sim, got {name!r}")
    admin = await asyncpg.connect(dsn_for("postgres"))
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
            await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    return dsn_for(name)
