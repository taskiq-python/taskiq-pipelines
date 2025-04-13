from typing import Any, Dict, List, Optional

import pydantic
from taskiq import (
    AsyncBroker,
    AsyncTaskiqTask,
    Context,
    TaskiqDepends,
    TaskiqMessage,
    TaskiqResult,
    async_shared_broker,
)

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA
from taskiq_pipelines.steps.mapper import wait_tasks


@async_shared_broker.task(task_name="taskiq_pipelines.shared.wait_group_tasks")
async def wait_group_tasks(
    task_ids: List[str],
    check_interval: float,
    skip_errors: bool = False,
    context: Context = TaskiqDepends(),
) -> tuple[Any, ...]:
    """Waits for subtasks to complete."""
    res = await wait_tasks(
        task_ids,
        check_interval=check_interval,
        skip_errors=False,
        none_if_errors=skip_errors,
        context=context,
    )
    return tuple(res)


class GroupStepItem(pydantic.BaseModel):
    """Item of a group step."""

    task_name: str
    labels: Dict[str, Any]
    labels_types: Optional[Dict[str, int]] = None
    args: List[Any]
    param_name: Optional[str]
    kwargs: Dict[str, Any]

    def from_message(self, message: TaskiqMessage) -> None:
        """
        Parse labels and kwargs from message.

        :param message: message to parse.
        """
        self.labels = message.labels
        self.labels_types = message.labels_types
        self.args = message.args
        self.kwargs = message.kwargs

    def to_message(
        self,
        task_id: str,
        result: Optional[TaskiqResult[Any]] = None,
    ) -> TaskiqMessage:
        """
        Convert this item to message.

        :return: message
        """
        args = self.args
        kwargs = self.kwargs
        if result:
            if self.param_name:
                kwargs[self.param_name] = result.return_value
            else:
                args = [result.return_value, *args]

        return TaskiqMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=self.labels,
            labels_types=self.labels_types,
            args=args,
            kwargs=kwargs,
        )


class GroupStep(pydantic.BaseModel, AbstractStep, step_name="group"):
    """Step that maps iterables."""

    tasks: list[GroupStepItem]
    skip_errors: bool
    check_interval: float
    pass_args: bool = False

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: bytes,
        result: "TaskiqResult[Any]",
    ) -> AsyncTaskiqTask[Any]:
        """
        Execute group action.

        This steps creates many small tasks
        and one waiter task.

        The waiter task awaits for all small tasks to complete,
        and then assembles the final result.
        """
        ids: List[str] = []
        for task in self.tasks:
            subtask_id = broker.id_generator()
            ids.append(subtask_id)

            if self.pass_args:
                message = task.to_message(subtask_id, result)
            else:
                message = task.to_message(subtask_id, None)

            await broker.kick(broker.formatter.dumps(message))

        return await (
            wait_group_tasks.kicker()
            .with_broker(broker)
            .with_task_id(task_id)
            .with_labels(
                **{CURRENT_STEP: step_number, PIPELINE_DATA: pipe_data},  # type: ignore
            )
            .kiq(
                task_ids=ids,
                skip_errors=self.skip_errors,
                check_interval=self.check_interval,
            )
        )
