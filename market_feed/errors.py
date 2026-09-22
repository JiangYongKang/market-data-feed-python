class MarketFeedError(Exception):
    """Base error for deterministic market-feed failures."""


class NormalizationError(MarketFeedError):
    pass


class SubscriptionError(MarketFeedError):
    pass


class BackpressureError(MarketFeedError):
    pass


class SequenceError(MarketFeedError):
    pass
