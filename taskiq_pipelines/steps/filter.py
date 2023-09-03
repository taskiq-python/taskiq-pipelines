import asyncio
from typing import Any, Dict, Iterable, List, Optional, Union

import pydantic
from taskiq import AsyncBroker, Context, TaskiqDepends, TaskiqError, TaskiqResult
from taskiq.brokers.shared_broker import async_shared_broker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA
from taskiq_pipelines.exceptions import AbortPipeline


@async_shared_broker.task(task_name="taskiq_pipelines.shared.filter_tasks")
async def filter_tasks(  # noqa: C901, WPS210, WPS231
    task_ids: List[str],
    parent_task_id: str,
    check_interval: float,
    skip_errors: bool = False,
    context: Context = TaskiqDepends(),
) -> List[Any]:
    """
    Filter resulted tasks.

    It takes list of task ids,
    and parent task id.

    After all subtasks are completed it gets
    result of a parent task, and
    if subtask's result of execution can be
    converted to True, the item from the original
    tasks is added to the resulting array.

    :param task_ids: ordered list of task ids.
    :param parent_task_id: task id of a parent task.
    :param check_interval: how often checks are performed.
    :param context: context of the execution, defaults to default_context
    :param skip_errors: skip errors of subtasks, defaults to False
    :raises TaskiqError: if any subtask has returned error.
    :return: fitlered results.
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

    results = await context.broker.result_backend.get_result(parent_task_id)
    filtered_results = []
    for task_id, value in zip(  # type: ignore  # noqa: WPS352, WPS440
        ordered_ids,
        results.return_value,
    ):
        result = await context.broker.result_backend.get_result(task_id)
        if result.is_err:
            if skip_errors:
                continue
            raise TaskiqError(f"Task {task_id} returned error. Filtering failed.")
        if result.return_value:
            filtered_results.append(value)
    return filtered_results


class FilterStep(pydantic.BaseModel, AbstractStep, step_name="filter"):
    """Task to filter results."""

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
    def loads(cls, data: str) -> "FilterStep":
        """
        Parses mapper step from string.

        :param data: dumped data.
        :return: parsed step.
        """
        return pydantic.TypeAdapter(FilterStep).validate_json(data)

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
        Run filter action.

        This function creates many small filter steps,
        and then collects all results in one big filtered array,
        using 'filter_tasks' shared task.

        :param broker: current broker.
        :param step_number: current step number.
        :param parent_task_id: task_id of the previous step.
        :param task_id: task_id to use in this step.
        :param pipe_data: serialized pipeline.
        :param result: result of the previous task.
        :raises AbortPipeline: if result is not iterable.
        """
        if not isinstance(result.return_value, Iterable):
            raise AbortPipeline("Result of the previous task is not iterable.")
        sub_task_ids = []
        for item in result.return_value:
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
        await filter_tasks.kicker().with_task_id(task_id).with_broker(
            broker,
        ).with_labels(
            **{CURRENT_STEP: step_number, PIPELINE_DATA: pipe_data},  # type: ignore
        ).kiq(
            sub_task_ids,
            parent_task_id,
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
    ) -> "FilterStep":
        """
        Create new filter step from task.

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
        return FilterStep(
            task_name=message.task_name,
            labels=message.labels,
            param_name=param_name,
            additional_kwargs=additional_kwargs,
            skip_errors=skip_errors,
            check_interval=check_interval,
        )
