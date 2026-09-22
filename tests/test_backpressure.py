import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_feed.enums import BackpressurePolicy, DeliveryKind, SourceId
from market_feed.models import DeliveryEvent, Quote, SubscriberOptions
from market_feed.dispatcher import Dispatcher


def quote(sequence: int, symbol: str = "AAA"):
    now = datetime(2026, 9, 22, tzinfo=timezone.utc)
    return Quote(
        source=SourceId.SOURCE_A,
        symbol=symbol,
        sequence=sequence,
        event_time=now,
        received_time=now,
        price=Decimal(sequence),
        quantity=Decimal(1),
        raw_record={"seq": sequence},
        record_id=f"r{sequence}",
    )


def event(payload, kind=DeliveryKind.QUOTE):
    return DeliveryEvent(kind=kind, timestamp=datetime.now(timezone.utc), payload=payload)


@pytest.mark.asyncio
async def test_drop_oldest_keeps_latest_and_audits_dropped_event():
    dispatcher = Dispatcher()
    await dispatcher.add_subscriber("slow", SubscriberOptions(queue_size=2, backpressure=BackpressurePolicy.DROP_OLDEST))
    audits = []
    for sequence in range(3):
        audits.extend(await dispatcher.publish(["slow"], event(quote(sequence))))

    first = await dispatcher.get("slow")
    second = await dispatcher.get("slow")
    assert [item.payload.sequence for item in (first, second)] == [1, 2]
    assert len(audits) == 1
    assert audits[0].subscriber_id == "slow"
    assert audits[0].sequence == 0
    assert "oldest" in audits[0].reason


@pytest.mark.asyncio
async def test_coalesce_latest_merges_only_same_key_and_kind_with_audit():
    dispatcher = Dispatcher()
    await dispatcher.add_subscriber("slow", SubscriberOptions(queue_size=3, backpressure=BackpressurePolicy.COALESCE_LATEST))
    audits = []
    audits.extend(await dispatcher.publish(["slow"], event(quote(1, "AAA"))))
    audits.extend(await dispatcher.publish(["slow"], event(quote(2, "BBB"))))
    audits.extend(await dispatcher.publish(["slow"], event(quote(3, "AAA"))))

    first = await dispatcher.get("slow")
    second = await dispatcher.get("slow")
    assert (first.payload.symbol, first.payload.sequence) == ("AAA", 3)
    assert (second.payload.symbol, second.payload.sequence) == ("BBB", 2)
    assert len(audits) == 1
    assert audits[0].dropped_kind is DeliveryKind.COALESCED
    assert audits[0].sequence == 1

    recovered = event(quote(4, "AAA"), DeliveryKind.RECOVERED_QUOTE)
    more_audits = await dispatcher.publish(["slow"], recovered)
    # Different DeliveryKind is not merged into a normal quote.
    assert more_audits == []
    third = await dispatcher.get("slow")
    assert third.kind is DeliveryKind.RECOVERED_QUOTE


@pytest.mark.asyncio
async def test_reject_policy_is_bounded_and_does_not_block():
    dispatcher = Dispatcher()
    await dispatcher.add_subscriber("slow", SubscriberOptions(queue_size=1, backpressure=BackpressurePolicy.REJECT))
    await dispatcher.publish(["slow"], event(quote(1)))
    audits = await dispatcher.publish(["slow"], event(quote(2)))

    assert len(audits) == 1
    assert audits[0].sequence == 2
    assert dispatcher.queue_snapshot() == {"slow": 1}


@pytest.mark.asyncio
async def test_block_policy_applies_backpressure_without_unbounded_growth():
    dispatcher = Dispatcher()
    await dispatcher.add_subscriber("slow", SubscriberOptions(queue_size=1, backpressure=BackpressurePolicy.BLOCK))

    async def publish_second():
        return await dispatcher.publish(["slow"], event(quote(2)))

    await dispatcher.publish(["slow"], event(quote(1)))
    publish_task = asyncio.create_task(publish_second())
    await asyncio.sleep(0.01)
    assert not publish_task.done()
    assert dispatcher.queue_snapshot() == {"slow": 1}

    first = await dispatcher.get("slow")
    audits = await publish_task
    second = await dispatcher.get("slow")
    assert first.payload.sequence == 1
    assert second.payload.sequence == 2
    assert audits == []
