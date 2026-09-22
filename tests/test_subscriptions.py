import pytest

from market_feed.enums import SourceId, SubscriptionErrorKind
from market_feed.models import SubscriptionKey
from market_feed.subscriptions import SubscriptionRegistry


AAA_A = SubscriptionKey(SourceId.SOURCE_A, "AAA")
AAA_B = SubscriptionKey(SourceId.SOURCE_B, "AAA")
UNKNOWN = SubscriptionKey(SourceId.SOURCE_A, "NOPE")


def test_subscribe_and_repeat_subscribe_is_idempotent_without_change():
    registry = SubscriptionRegistry()
    first = registry.subscribe("s1", AAA_A)
    second = registry.subscribe("s1", AAA_A)

    assert first.ok and first.changed
    assert second.ok and not second.changed
    assert registry.subscribers_for(AAA_A) == ("s1",)


def test_unsubscribe_missing_subscription_is_idempotent_and_does_not_affect_other_key():
    registry = SubscriptionRegistry()
    registry.subscribe("s1", AAA_A)

    result = registry.unsubscribe("s1", AAA_B)

    assert result.ok and not result.changed
    assert registry.subscribers_for(AAA_A) == ("s1",)
    assert registry.subscribers_for(AAA_B) == ()


def test_unknown_symbol_is_rejected_and_existing_subscription_remains_intact():
    registry = SubscriptionRegistry()
    existing = registry.subscribe("s1", AAA_A)
    rejected = registry.subscribe("s1", UNKNOWN)

    assert existing.ok
    assert not rejected.ok
    assert rejected.kind is SubscriptionErrorKind.UNKNOWN_SYMBOL
    assert registry.subscribers_for(AAA_A) == ("s1",)
    assert "s1" in registry.snapshot()


def test_multiple_subscribers_are_independent_and_sorted():
    registry = SubscriptionRegistry()
    registry.subscribe("b", AAA_A)
    registry.subscribe("a", AAA_A)
    registry.subscribe("a", AAA_B)

    assert registry.subscribers_for(AAA_A) == ("a", "b")
    assert registry.subscribers_for(AAA_B) == ("a",)

    assert registry.unsubscribe("b", AAA_A).changed is True
    assert registry.subscribers_for(AAA_A) == ("a",)


def test_invalid_subscriber_is_rejected():
    registry = SubscriptionRegistry()
    result = registry.subscribe(" ", AAA_A)
    assert not result.ok
    assert result.kind is SubscriptionErrorKind.INVALID_SUBSCRIBER
