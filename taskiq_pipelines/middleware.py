from logging import getLogger
from typing import Any, List

import pydantic
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA
from taskiq_pipelines.exceptions import AbortPipeline
from taskiq_pipelines.pipeliner import DumpedStep
from taskiq_pipelines.steps import parse_step

logger = getLogger(__name__)


class PipelineMiddleware(TaskiqMiddleware):
    """Pipeline middleware."""

    async def post_save(  # noqa: C901, WPS212
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Handle post-execute event.

        This is the heart of pipelines.
        Here we decide what to do next.

        If the message have pipeline
        labels we can calculate our next step.

        :param message: current message.
        :param result: result of the execution.
        """
        if result.is_err:
            return
        if CURRENT_STEP not in message.labels:
            return
        current_step_num = int(message.labels[CURRENT_STEP])
        if PIPELINE_DATA not in message.labels:
            logger.warn("Pipline data not found. Execution flow is broken.")
            return
        pipeline_data = message.labels[PIPELINE_DATA]
        try:
            steps_data = pydantic.TypeAdapter(List[DumpedStep]).validate_json(
                pipeline_data,
            )
        except ValueError:
            return
        if current_step_num + 1 >= len(steps_data):
            logger.debug("Pipeline is completed.")
            return
        next_step_data = steps_data[current_step_num + 1]
        try:
            next_step = parse_step(
                step_type=next_step_data.step_type,
                step_data=next_step_data.step_data,
            )
        except ValueError as exc:
            logger.warning("Cannot parse step data.")
            logger.debug("%s", exc, exc_info=True)
            return

        try:
            await next_step.act(
                broker=self.broker,
                step_number=current_step_num + 1,
                parent_task_id=message.task_id,
                task_id=next_step_data.task_id,
                pipe_data=pipeline_data,
                result=result,
            )
        except AbortPipeline as abort_exc:
            logger.warning(
                "Pipeline is aborted. Reason: %s",
                abort_exc,
                exc_info=True,
            )
            if current_step_num == len(steps_data) - 1:
                return
            await self.fail_pipeline(steps_data[-1].task_id)

    async def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """
        Handles on_error event.

        :param message: current message.
        :param result: execution result.
        :param exception: found exception.
        """
        if CURRENT_STEP not in message.labels:
            return
        current_step_num = int(message.labels[CURRENT_STEP])
        if PIPELINE_DATA not in message.labels:
            logger.warn("Pipline data not found. Execution flow is broken.")
            return
        pipe_data = message.labels[PIPELINE_DATA]
        try:
            steps = pydantic.TypeAdapter(List[DumpedStep]).validate_json(pipe_data)
        except ValueError:
            return
        if current_step_num == len(steps) - 1:
            return
        await self.fail_pipeline(steps[-1].task_id)

    async def fail_pipeline(self, last_task_id: str) -> None:
        """
        This function aborts pipeline.

        This is done by setting error result for
        the last task in the pipeline.

        :param last_task_id: id of the last task.
        """
        await self.broker.result_backend.set_result(
            last_task_id,
            TaskiqResult(
                is_err=True,
                return_value=None,  # type: ignore
                error=AbortPipeline("Execution aborted."),
                execution_time=0,
                log="Error found while executing pipeline.",
            ),
        )
