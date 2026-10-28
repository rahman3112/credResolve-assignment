"""Provider events -> database, including the agent and borrower consequences.

`apply_provider_event` (repo/events.py) decides whether an event is news and moves the CALL.
The hook below runs inside that same transaction and moves everything the call drags along
(agent, borrower), so a crash can never leave "call ANSWERED but agent still DIALING".
"""

from __future__ import annotations

from datetime import timedelta

from smartdialer.dialer.completion import hangup_quietly, must, release_after_call
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.stats import DialerStats
from smartdialer.repo.agents import get_agent, reserve_agent, transition_agent
from smartdialer.repo.events import ProviderEvent, apply_provider_event
from smartdialer.repo.models import Call
from smartdialer.repo.calls import transition_call
from smartdialer.scheduler import Scheduler
from smartdialer.states import TERMINAL_CALL_STATES, AgentState, CallState


class EventProcessor:
    def __init__(self, pool, sched: Scheduler, provider, cfg: DialerConfig, stats: DialerStats):
        self.pool, self.sched, self.provider, self.cfg, self.stats = pool, sched, provider, cfg, stats

    async def handle(self, ev: ProviderEvent) -> None:
        """The provider's callback. Safe under duplicates, reordering and concurrency."""
        post: list = []  # provider calls to make AFTER the transaction commits
        now = self.sched.now()

        async def on_applied(conn, before: Call, after: Call) -> None:
            await self._on_applied(conn, before, after, now, post)

        async with self.pool.acquire() as conn:
            res = await apply_provider_event(conn, ev, now, on_applied, lease_seconds=self.cfg.lease_seconds)
        self.stats.events_applied[res.outcome] += 1
        for provider_call_id in post:
            self.sched.spawn(hangup_quietly(self.provider, provider_call_id, self.stats))

    async def _on_applied(self, conn, before: Call, after: Call, now, post: list) -> None:
        if after.state is CallState.ANSWERED:
            await self._bridge_or_abandon(conn, after, now, post)
        elif after.state in TERMINAL_CALL_STATES:
            # COMPLETED / FAILED. If the call ended without ever being connected (e.g. COMPLETED
            # overtook ANSWERED) release_after_call sends the agent straight back to AVAILABLE.
            await release_after_call(conn, after, now, self.cfg)
        # RINGING: nothing to do; the call lease was renewed by apply_provider_event.

    async def _bridge_or_abandon(self, conn, call: Call, now, post: list) -> None:
        """Borrower picked up (progressive: the agent is already held). Bridge them, or, if
        the agent is no longer there, this answered call is an ABANDON: record it as one."""
        if call.agent_id is None:
            await self._bind_agent_or_abandon(conn, call, now, post)
            return
        agent = await get_agent(conn, call.agent_id, for_update=True)  # lock order: call -> agent
        if agent.state is AgentState.DIALING:
            lease = now + timedelta(seconds=self.cfg.connected_lease_seconds)
            await must(transition_call(conn, call, CallState.CONNECTED, now, lease_until=lease), "call->CONNECTED")
            await must(transition_agent(conn, agent, AgentState.CONNECTED, now), "agent->CONNECTED")
            return
        ended = await must(transition_call(conn, call, CallState.FAILED, now, end_reason="ABANDONED"),
                           "call->FAILED(ABANDONED)")
        await release_after_call(conn, ended, now, self.cfg)
        self.stats.abandons += 1
        if call.provider_call_id:
            post.append(call.provider_call_id)

    async def _bind_agent_or_abandon(self, conn, call: Call, now, post: list) -> None:
        """PREDICTIVE: the borrower answered and no agent was held for them. Take the longest-idle
        free agent in ONE atomic statement (the same SKIP LOCKED reservation progressive uses, so a
        burst of simultaneous answers each get a different agent and never share one). All inside
        the event's transaction: the call, the agent and the borrower change together.

        No free agent -> this answered call is an ABANDON, recorded as one (it is the number the
        compliance budget is made of) and hung up after commit. Which of the two happens is decided
        by the database at this instant; nothing earlier in the pipeline can promise either."""
        agent = await reserve_agent(conn, call.campaign_id, now, now + timedelta(seconds=self.cfg.lease_seconds))
        if agent is not None:
            lease = now + timedelta(seconds=self.cfg.connected_lease_seconds)
            await must(transition_call(conn, call, CallState.CONNECTED, now, lease_until=lease, agent_id=agent.id),
                       "call->CONNECTED(late-bound)")
            await must(transition_agent(conn, agent, AgentState.CONNECTED, now), "agent->CONNECTED")
            self.stats.late_bound += 1
            return
        ended = await must(transition_call(conn, call, CallState.FAILED, now, end_reason="ABANDONED"),
                           "call->FAILED(ABANDONED)")
        await release_after_call(conn, ended, now, self.cfg)
        self.stats.abandons += 1
        if call.provider_call_id:
            post.append(call.provider_call_id)
