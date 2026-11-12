import asyncio
from typing import Any

import pytest
from taskiq import (
    InMemoryBroker,
    SimpleRetryMiddleware,
    TaskiqMessage,
    TaskiqMiddleware,
    TaskiqResult,
)
from taskiq.exceptions import NoResultError

from taskiq_pipelines import Pipeline, PipelineMiddleware


class AttemptObserver(TaskiqMiddleware):
    """Observe an attempt after every middleware has processed its error."""

    def __init__(self) -> None:
        self.failed_attempt_finished = asyncio.Event()

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """Notify the test after the failing attempt has finished error hooks."""
        if result.is_err:
            self.failed_attempt_finished.set()


@pytest.mark.parametrize("pipeline_first", [True, False])
async def test_retry_does_not_publish_a_premature_pipeline_error(
    pipeline_first: bool,
) -> None:
    observer = AttemptObserver()
    middlewares: list[TaskiqMiddleware] = [
        PipelineMiddleware(),
        SimpleRetryMiddleware(default_retry_count=3),
    ]
    if not pipeline_first:
        middlewares.reverse()
    broker = InMemoryBroker().with_middlewares(observer, *middlewares)
    retry_started = asyncio.Event()
    release_retry = asyncio.Event()
    attempts = 0
    following_calls: list[int] = []

    @broker.task(retry_on_error=True)
    async def flaky(value: int) -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("temporary failure")
        retry_started.set()
        await release_retry.wait()
        return value + 1

    @broker.task
    async def following(value: int) -> int:
        following_calls.append(value)
        return value * 2

    try:
        sent = await Pipeline(broker, flaky).call_next(following).kiq(1)
        await asyncio.wait_for(observer.failed_attempt_finished.wait(), timeout=3)
        await asyncio.wait_for(retry_started.wait(), timeout=3)
        assert not await broker.result_backend.is_result_ready(sent.task_id)
        assert following_calls == []

        release_retry.set()
        result = await asyncio.wait_for(
            sent.wait_result(check_interval=0.01),
            timeout=3,
        )
        assert not result.is_err
        assert result.return_value == 4
        assert attempts == 2
        assert following_calls == [2]
    finally:
        release_retry.set()
        await broker.shutdown()


@pytest.mark.parametrize("pipeline_first", [True, False])
@pytest.mark.parametrize("retry_on_error", [True, False])
async def test_terminal_failure_still_fails_the_pipeline(
    pipeline_first: bool,
    retry_on_error: bool,
) -> None:
    middlewares: list[TaskiqMiddleware] = [
        PipelineMiddleware(),
        SimpleRetryMiddleware(default_retry_count=3),
    ]
    if not pipeline_first:
        middlewares.reverse()
    broker = InMemoryBroker().with_middlewares(*middlewares)
    attempts = 0
    following_calls: list[int] = []

    @broker.task(retry_on_error=retry_on_error)
    async def failing(value: int) -> int:
        nonlocal attempts
        attempts += 1
        raise ValueError("terminal failure")

    @broker.task
    async def following(value: int) -> int:
        following_calls.append(value)
        return value

    try:
        sent = await Pipeline(broker, failing).call_next(following).kiq(1)
        result = await asyncio.wait_for(
            sent.wait_result(check_interval=0.01),
            timeout=3,
        )
        assert result.is_err
        assert isinstance(result.error, ValueError)
        assert str(result.error) == "terminal failure"
        assert attempts == (3 if retry_on_error else 1)
        assert following_calls == []
    finally:
        await broker.shutdown()


async def test_no_result_does_not_fail_the_pipeline() -> None:
    observer = AttemptObserver()
    broker = InMemoryBroker().with_middlewares(observer, PipelineMiddleware())

    @broker.task
    async def suppressed() -> int:
        raise NoResultError

    @broker.task
    async def following(value: int) -> int:
        return value

    try:
        sent = await Pipeline(broker, suppressed).call_next(following).kiq()
        await asyncio.wait_for(observer.failed_attempt_finished.wait(), timeout=3)
        assert not await broker.result_backend.is_result_ready(sent.task_id)
    finally:
        await broker.shutdown()


async def test_last_step_failure_keeps_its_saved_result() -> None:
    broker = InMemoryBroker().with_middlewares(PipelineMiddleware())

    @broker.task
    async def start() -> int:
        return 1

    @broker.task
    async def failing(value: int) -> int:
        raise ValueError("last step failed")

    try:
        sent = await Pipeline(broker, start).call_next(failing).kiq()
        result = await asyncio.wait_for(
            sent.wait_result(check_interval=0.01),
            timeout=3,
        )
        assert result.is_err
        assert isinstance(result.error, ValueError)
        assert str(result.error) == "last step failed"
        assert result.labels["_pipe_current_step"] == 1
    finally:
        await broker.shutdown()
