"""Agent presence changes: login, logout, break, and (the interesting one) vanishing.

How "the agent disappears" reaches us is outside this prototype (a softphone heartbeat
timing out, a websocket closing, a supervisor forcing a logout): all of them call
`agent_offline()`. The dialer's reaction time is therefore the detection latency plus ONE
database transaction; there is no polling delay on our side.
"""

from __future__ import annotations

from smartdialer.dialer.completion import hangup_quietly, must, release_after_call
from smartdialer.dialer.config import DialerConfig
from smartdialer.dialer.stats import DialerStats
from smartdialer.repo.agents import get_agent, request_pause, transition_agent
from smartdialer.repo.calls import get_call, live_call_for_agent, transition_call
from smartdialer.scheduler import Scheduler
from smartdialer.states import AgentState, CallState


class AgentLifecycle:
    def __init__(self, pool, sched: Scheduler, provider, cfg: DialerConfig, stats: DialerStats):
        self.pool, self.sched, self.provider, self.cfg, self.stats = pool, sched, provider, cfg, stats

    async def agent_login(self, agent_id: int) -> bool:
        async with self.pool.acquire() as conn, conn.transaction():
            agent = await get_agent(conn, agent_id, for_update=True)
            if agent.state not in (AgentState.OFFLINE, AgentState.PAUSED):
                return False
            return await transition_agent(conn, agent, AgentState.AVAILABLE, self.sched.now()) is not None

    async def agent_pause(self, agent_id: int) -> str:
        async with self.pool.acquire() as conn:
            return await request_pause(conn, agent_id, self.sched.now())

    async def agent_offline(self, agent_id: int) -> None:
        """The agent is gone. Whatever call they were holding is ended correctly:

        reserved / initiated / ringing -> CANCELLED (AGENT_LOST). Not an abandon: the borrower
                                           never picked up. The borrower is refunded and retried.
        answered, not yet bridged      -> FAILED (ABANDONED). The borrower is on the line with
                                           nobody: this IS an abandon and counts as one.
        connected                      -> FAILED (AGENT_DROPPED): dropped mid-conversation.

        Lock order is call -> agent -> borrower everywhere (see completion.py). We learn the call
        from the agent, so: find it unlocked, lock call then agent, and re-verify; if a call
        appeared meanwhile, start over.
        """
        for _ in range(5):
            post = await self._try_offline(agent_id)
            if post is not None:
                break
        else:
            raise RuntimeError(f"agent {agent_id}: could not settle live call under contention")
        # Reaction time is the DB transaction above. The provider hangup is slower (network) and
        # must not delay the caller, so it runs as a background task.
        for provider_call_id in post:
            self.sched.spawn(hangup_quietly(self.provider, provider_call_id, self.stats))

    async def _try_offline(self, agent_id: int) -> list[str] | None:
        now = self.sched.now()
        async with self.pool.acquire() as conn, conn.transaction():
            peek = await live_call_for_agent(conn, agent_id)
            call = await get_call(conn, peek.id, for_update=True) if peek else None
            agent = await get_agent(conn, agent_id, for_update=True)
            current = await live_call_for_agent(conn, agent_id)
            if (current.id if current else None) != (call.id if call else None):
                return None  # a call was reserved/finished while we looked: retry
            if agent.state is AgentState.OFFLINE:
                return []
            await must(transition_agent(conn, agent, AgentState.OFFLINE, now), "agent->OFFLINE")
            if call is None or call.agent_id != agent_id:
                return []
            if call.state in (CallState.RESERVED, CallState.INITIATED, CallState.RINGING):
                ended = await must(transition_call(conn, call, CallState.CANCELLED, now, end_reason="AGENT_LOST"),
                                   "call->CANCELLED")
            elif call.state is CallState.ANSWERED:
                ended = await must(transition_call(conn, call, CallState.FAILED, now, end_reason="ABANDONED"),
                                   "call->FAILED(ABANDONED)")
            else:  # CONNECTED
                ended = await must(transition_call(conn, call, CallState.FAILED, now, end_reason="AGENT_DROPPED"),
                                   "call->FAILED(AGENT_DROPPED)")
            await release_after_call(conn, ended, now, self.cfg)  # agent already OFFLINE: only the borrower moves
            # If we have no provider id yet, place_call is still in flight; the allocator
            # sees the terminal call when it returns and hangs up itself (compensation).
            return [call.provider_call_id] if call.provider_call_id else []
