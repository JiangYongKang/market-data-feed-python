import pytest

from market_feed.enums import SourceId
from market_feed.sources import ScriptedSource


@pytest.mark.asyncio
async def test_scripted_source_batches_seek_and_disconnect_are_local_and_deterministic():
    records = [{"seq": index} for index in range(5)]
    source = ScriptedSource(SourceId.SOURCE_A, records)

    with pytest.raises(RuntimeError):
        await source.fetch_batch()

    await source.connect()
    assert await source.fetch_batch(2) == [{"seq": 0}, {"seq": 1}]
    assert await source.fetch_batch(2) == [{"seq": 2}, {"seq": 3}]
    assert source.cursor == 4

    source.seek(2)
    assert source.seek_sequence("seq", 4) is True
    assert await source.fetch_batch() == [{"seq": 4}]

    await source.disconnect()
    with pytest.raises(RuntimeError):
        await source.fetch_batch()
