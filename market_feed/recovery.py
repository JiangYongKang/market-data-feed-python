"""Compatibility surface for bounded gap recovery."""

from __future__ import annotations

from .models import Quote, SequenceState, SubscriptionKey
from .sequence import SequenceTracker


class GapRecoveryTracker(SequenceTracker):
    """Sequence and timestamp tracker with an explicit recovery window."""

    def observe_recovery(self, quote: Quote):
        decision = self.observe(quote)
        return list(decision.release), list(decision.gap_events)

    def snapshot(self) -> SequenceState:
        return super().snapshot()
