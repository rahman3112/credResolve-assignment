"""One dialer worker: the loop that wires the pieces together.

    tick:  free finished wrap-ups -> snapshot -> Pacer.propose -> SafetyController.review
           -> CallAllocator.execute(permit)

Any number of workers may run this against the same campaign: every contended step is an
atomic database statement (reservation) or a compare-and-set, so workers need no
coordination with each other, and a worker that dies simply stops ticking.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from smartdialer.dialer.allocator import CallAllocator
from smartdialer.dialer.callbacks import EventProcessor
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.lifecycle import AgentLifecycle
from smartdialer.dialer.pacing import Pacer, Proposal
from smartdialer.dialer.safety import Decision, SafetyController
from smartdialer.dialer.stats import DialerStats
from smartdialer.repo.agents import end_expired_wrap_ups
from smartdialer.repo.models import Mode
from smartdialer.repo.snapshot import load_snapshot
from smartdialer.scheduler import Scheduler


@dataclass(frozen=True, slots=True)
class TickReport:
    proposal: Proposal
    decision: Decision | None  # None when nothing was proposed
    started: int


class DialerWorker:
    def __init__(self, *, pool, sched: Scheduler, campaign_id: int, provider, pacer: Pacer,
                 safety: SafetyController, cfg: DialerConfig | None = None, name: str = "w1",
                 stats: DialerStats | None = None):
        self.pool, self.sched, self.campaign_id, self.name = pool, sched, campaign_id, name
        self.pacer, self.safety = pacer, safety
        self.cfg = cfg or DialerConfig()
        self.stats = stats or DialerStats()
        self.allocator = CallAllocator(pool, sched, provider, safety, self.cfg, self.stats)
        self.events = EventProcessor(pool, sched, provider, self.cfg, self.stats)
        self.agents = AgentLifecycle(pool, sched, provider, self.cfg, self.stats)
        self.allocator.events = self.events
        self.provider = provider

    def attach_event_handler(self) -> None:
        """Route the provider's callbacks to this worker. In production the callback is an HTTP
        endpoint behind a load balancer: ANY worker can serve ANY event, because all state is in
        PostgreSQL. (The mock has one handler slot, so tests attach it once.)"""
        self.provider.set_event_handler(self.events.handle)

    async def tick(self) -> TickReport:
        now = self.sched.now()
        self.stats.ticks += 1
        async with self.pool.acquire() as conn:
            await end_expired_wrap_ups(conn, self.campaign_id, now)
            snap = await load_snapshot(conn, self.campaign_id, now)
        proposal = self.pacer.propose(snap)
        # A predictive campaign is reviewed every tick even when nothing is proposed: the
        # controller may need to order excess ringing calls hung up (agents dropped, rate surged).
        if proposal.n <= 0 and snap.mode is not Mode.PREDICTIVE:
            return TickReport(proposal, None, 0)
        decision = await self.safety.review(proposal, self.campaign_id, now)
        self.stats.decisions[decision.verdict] += 1
        if decision.hangup_n:
            await self.allocator.hang_up_excess(self.campaign_id, decision.hangup_n)
        started = await self.allocator.execute(decision.permit) if decision.permit else 0
        return TickReport(proposal, decision, started)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.tick()
            await self.sched.sleep(self.cfg.tick_seconds)
