"""Worker-process helpers. Kept out of __main__ so spawned processes can import them by name."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from smartdialer.sim.runner import RunSpec, run_scenario

_slot: int | None = None


def init_worker(counter) -> None:
    """Give each worker process a distinct slot so each uses its own throwaway database."""
    global _slot
    with counter.get_lock():
        _slot = counter.value
        counter.value += 1


def run_in_process(spec: RunSpec) -> dict:
    return asyncio.run(run_scenario(replace(spec, db_name=f"dialer_{_slot}_sim")))
