from typing import List

import pytest
from taskiq import InMemoryBroker

from taskiq_pipelines import Pipeline, PipelineMiddleware


@pytest.mark.anyio
async def test_success() -> None:
    """Tests that sequential step works as expected."""
    broker = InMemoryBroker().with_middlewares(PipelineMiddleware())

    @broker.task
    def add(i: int) -> int:
        return i + 1

    @broker.task
    def double(i: int) -> int:
        return i * 2

    pipe = Pipeline(broker, add).call_next(double)
    sent = await pipe.kiq(1)
    res = await sent.wait_result()
    assert res.return_value == 4


@pytest.mark.anyio
async def test_mapping_success() -> None:
    """Test that map step works as expected."""
    broker = InMemoryBroker().with_middlewares(PipelineMiddleware())

    @broker.task
    def ranger(i: int) -> List[int]:
        return list(range(i))

    @broker.task
    def double(i: int) -> int:
        return i * 2

    pipe = Pipeline(broker, ranger).map(double)
    sent = await pipe.kiq(4)
    res = await sent.wait_result()
    assert res.return_value == list(map(double, ranger(4)))
