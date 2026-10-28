"""Translate what the provider says is true NOW into the events we may have missed.

Used wherever we ask the provider instead of waiting for a callback: the reconciler (settling
calls whose lease expired) and the excess-hangup path (verifying a call is really still ringing
before we cancel it). One translation, so both agree on what each provider state implies.

Monotonic by construction: each event is a fact that was already true, and the normal rank rules
ignore any that are behind us. `seq = -1` marks "observed by us", so observing the same fact twice
is a deduplicated no-op.
"""

from __future__ import annotations

from smartdialer.providers.base import ProviderCallState, ProviderCallStatus
from smartdialer.repo.events import ProviderEvent, ProviderEventType

OBSERVED_SEQ = -1


def events_for_status(provider_name: str, call_id: int, st: ProviderCallStatus) -> list[ProviderEvent]:
    def ev(t: ProviderEventType, reason: str | None = None) -> ProviderEvent:
        return ProviderEvent(provider_name, st.provider_call_id, t, seq=OBSERVED_SEQ, client_ref=call_id,
                             reason=reason, payload={"source": "observed"})

    match st.state:
        case ProviderCallState.RINGING:
            return [ev(ProviderEventType.RINGING)]
        case ProviderCallState.ANSWERED:
            return [ev(ProviderEventType.ANSWERED)]
        case ProviderCallState.COMPLETED:
            # It ended normally, which means the borrower answered and talked, even if we saw none of
            # it: replay both so the agent is bridged, wrapped up and credited.
            return ([ev(ProviderEventType.ANSWERED)] if st.was_answered else []) + [ev(ProviderEventType.COMPLETED)]
        case ProviderCallState.FAILED:
            return [ev(ProviderEventType.FAILED, st.reason or "PROVIDER_ERROR")]
    return []  # INITIATED at the provider: nothing new to say
