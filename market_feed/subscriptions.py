"""Thread-safe subscription state."""

from __future__ import annotations

from threading import RLock

from .enums import SourceId, SubscriptionErrorKind
from .models import SubscriptionKey, SubscriptionResult


class SubscriptionRegistry:
    """Maintain subscriber -> keys and key -> subscriber indexes atomically."""

    def __init__(self, known_symbols: dict[SourceId, frozenset[str]] | None = None) -> None:
        default = frozenset({"AAA", "BBB"})
        self._known_symbols = known_symbols or {
            SourceId.SOURCE_A: default,
            SourceId.SOURCE_B: default,
        }
        self._subscriber_keys: dict[str, set[SubscriptionKey]] = {}
        self._key_subscribers: dict[SubscriptionKey, set[str]] = {}
        self._lock = RLock()

    def _validate(self, subscriber_id: str, key: SubscriptionKey) -> SubscriptionResult | None:
        if not isinstance(subscriber_id, str) or not subscriber_id.strip():
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id if isinstance(subscriber_id, str) else "",
                key=None,
                kind=SubscriptionErrorKind.INVALID_SUBSCRIBER,
                detail="subscriber_id must be a non-empty string",
                changed=False,
            )
        if key.source not in self._known_symbols:
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=SubscriptionErrorKind.UNKNOWN_SOURCE,
                detail=f"unknown source: {key.source.value}",
                changed=False,
            )
        if key.symbol not in self._known_symbols[key.source]:
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id,
                key=key,
                kind=SubscriptionErrorKind.UNKNOWN_SYMBOL,
                detail=f"unknown symbol {key.symbol!r} for source {key.source.value}",
                changed=False,
            )
        return None

    def subscribe(self, subscriber_id: str, key: SubscriptionKey) -> SubscriptionResult:
        invalid = self._validate(subscriber_id, key)
        if invalid is not None:
            return invalid
        with self._lock:
            existing = self._subscriber_keys.setdefault(subscriber_id, set())
            if key in existing:
                return SubscriptionResult(
                    ok=True,
                    subscriber_id=subscriber_id,
                    key=key,
                    kind=None,
                    detail="subscription already exists; request is idempotent",
                    changed=False,
                )
            # Both indexes are changed while the same lock is held.
            existing.add(key)
            self._key_subscribers.setdefault(key, set()).add(subscriber_id)
            return SubscriptionResult(
                ok=True,
                subscriber_id=subscriber_id,
                key=key,
                kind=None,
                detail="subscribed",
                changed=True,
            )

    def unsubscribe(self, subscriber_id: str, key: SubscriptionKey) -> SubscriptionResult:
        invalid = self._validate(subscriber_id, key)
        if invalid is not None:
            return invalid
        with self._lock:
            keys = self._subscriber_keys.get(subscriber_id, set())
            if key not in keys:
                return SubscriptionResult(
                    ok=True,
                    subscriber_id=subscriber_id,
                    key=key,
                    kind=None,
                    detail="subscription does not exist; unsubscribe is idempotent",
                    changed=False,
                )
            keys.remove(key)
            if not keys:
                self._subscriber_keys.pop(subscriber_id, None)
            subscribers = self._key_subscribers.get(key)
            if subscribers is not None:
                subscribers.discard(subscriber_id)
                if not subscribers:
                    self._key_subscribers.pop(key, None)
            return SubscriptionResult(
                ok=True,
                subscriber_id=subscriber_id,
                key=key,
                kind=None,
                detail="unsubscribed",
                changed=True,
            )

    def is_known_key(self, key: SubscriptionKey) -> bool:
        return key.source in self._known_symbols and key.symbol in self._known_symbols[key.source]

    def validation_error_kind(self, key: SubscriptionKey) -> SubscriptionErrorKind | None:
        if key.source not in self._known_symbols:
            return SubscriptionErrorKind.UNKNOWN_SOURCE
        if key.symbol not in self._known_symbols[key.source]:
            return SubscriptionErrorKind.UNKNOWN_SYMBOL
        return None

    def subscribers_for(self, key: SubscriptionKey) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._key_subscribers.get(key, set())))

    def remove_subscriber(self, subscriber_id: str) -> SubscriptionResult:
        if not isinstance(subscriber_id, str) or not subscriber_id.strip():
            return SubscriptionResult(
                ok=False,
                subscriber_id=subscriber_id if isinstance(subscriber_id, str) else "",
                key=None,
                kind=SubscriptionErrorKind.INVALID_SUBSCRIBER,
                detail="subscriber_id must be a non-empty string",
                changed=False,
            )
        with self._lock:
            keys = set(self._subscriber_keys.get(subscriber_id, set()))
            for key in keys:
                self.unsubscribe(subscriber_id, key)
            return SubscriptionResult(
                ok=True,
                subscriber_id=subscriber_id,
                key=None,
                kind=None,
                detail=f"removed {len(keys)} subscription(s)",
                changed=bool(keys),
            )

    def snapshot(self) -> dict[str, list[SubscriptionKey]]:
        with self._lock:
            return {
                subscriber_id: sorted(keys, key=lambda key: (key.source.value, key.symbol))
                for subscriber_id, keys in sorted(self._subscriber_keys.items())
            }
