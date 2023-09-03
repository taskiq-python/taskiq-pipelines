import asyncio
from typing import Any, Dict, Iterable, List, Optional, Union

import pydantic
from taskiq import (
    AsyncBroker,
    AsyncTaskiqDecoratedTask,
    Context,
    TaskiqDepends,
    TaskiqError,
    TaskiqResult,
    async_shared_broker,
)
from taskiq.kicker import AsyncKicker

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA
from taskiq_pipelines.exceptions import AbortPipeline


@async_shared_broker.task(task_name="taskiq_pipelines.shared.wait_tasks")
async def wait_tasks(  # noqa: C901, WPS231
    task_ids: List[str],
    check_interval: float,
    skip_errors: bool = True,
    context: Context = TaskiqDepends(),
) -> List[Any]:
    """
    Waits for subtasks to complete.

    This function is used by mapper
    step.

    It awaits for all tasks from task_ids
    to complete and then collects results
    in single list.

    :param task_ids: list of task ids.
    :param check_interval: how often task completions are checked.
    :param context: current execution context, defaults to default_context
    :param skip_errors: doesn't fail pipeline if error is found.
    :raises TaskiqError: if error is found and skip_erros is false.
    :return: list of results.
    """
    ordered_ids = task_ids[:]
    tasks_set = set(task_ids)
    while tasks_set:
        for task_id in task_ids:  # noqa: WPS327
            if await context.broker.result_backend.is_result_ready(task_id):
                try:
                    tasks_set.remove(task_id)
                except LookupError:
                    continue
        if tasks_set:
            await asyncio.sleep(check_interval)

    results = []
    for task_id in ordered_ids:  # noqa: WPS440
        result = await context.broker.result_backend.get_result(task_id)
        if result.is_err:
            if skip_errors:
                continue
            raise TaskiqError(f"Task {task_id} returned error. Mapping failed.")
        results.append(result.return_value)
    return results


class MapperStep(pydantic.BaseModel, AbstractStep, step_name="mapper"):
    """Step that maps iterables."""

    task_name: str
    labels: Dict[str, str]
    param_name: Optional[str]
    additional_kwargs: Dict[str, Any]
    skip_errors: bool
    check_interval: float

    def dumps(self) -> str:
        """
        Dumps step as string.

        :return: returns json.
        """
        return self.model_dump_json()

    @classmethod
    def loads(cls, data: str) -> "MapperStep":
        """
        Parses mapper step from string.

        :param data: dumped data.
        :return: parsed step.
        """
        return pydantic.TypeAdapter(MapperStep).validate_json(data)

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Runs mapping.

        This step creates many small
        tasks and one waiter task.

        The waiter task awaits
        for all small tasks to complete,
        and then assembles the final result.

        :param broker: current broker.
        :param step_number: current step number.
        :param task_id: waiter task_id.
        :param parent_task_id: task_id of the previous step.
        :param pipe_data: serialized pipeline.
        :param result: result of the previous task.
        :raises AbortPipeline: if the result of the
            previous task is not iterable.
        """
        sub_task_ids: List[str] = []
        return_value = result.return_value
        if not isinstance(return_value, Iterable):
            raise AbortPipeline("Result of the previous task is not iterable.")

        for item in return_value:
            kicker: "AsyncKicker[Any, Any]" = AsyncKicker(
                task_name=self.task_name,
                broker=broker,
                labels=self.labels,
            )
            if self.param_name:
                self.additional_kwargs[self.param_name] = item
                task = await kicker.kiq(**self.additional_kwargs)
            else:
                task = await kicker.kiq(item, **self.additional_kwargs)
            sub_task_ids.append(task.task_id)

        await wait_tasks.kicker().with_task_id(task_id).with_broker(
            broker,
        ).with_labels(
            **{CURRENT_STEP: step_number, PIPELINE_DATA: pipe_data},  # type: ignore
        ).kiq(
            sub_task_ids,
            check_interval=self.check_interval,
            skip_errors=self.skip_errors,
        )

    @classmethod
    def from_task(
        cls,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        param_name: Optional[str],
        skip_errors: bool,
        check_interval: float,
        **additional_kwargs: Any,
    ) -> "MapperStep":
        """
        Create new mapper step from task.

        :param task: task to execute.
        :param param_name: parameter name.
        :param skip_errors: don't fail collector
            task on errors.
        :param check_interval: how often tasks are checked.
        :param additional_kwargs: additional function's kwargs.
        :return: new mapper step.
        """
        if isinstance(task, AsyncTaskiqDecoratedTask):
            kicker = task.kicker()
        else:
            kicker = task
        message = kicker._prepare_message()  # noqa: WPS437
        return MapperStep(
            task_name=message.task_name,
            labels=message.labels,
            param_name=param_name,
            additional_kwargs=additional_kwargs,
            skip_errors=skip_errors,
            check_interval=check_interval,
        )
