from typing import Any, Dict, Optional, Union

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask, TaskiqResult
from taskiq.kicker import AsyncKicker

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.constants import CURRENT_STEP, EMPTY_PARAM_NAME, PIPELINE_DATA


class SequentialStep(pydantic.BaseModel, AbstractStep, step_name="sequential"):
    """
    Step that's simply runs next function.

    It passes the result of the previous function
    as the first argument or as the keyword argument,
    if param_name is specified.
    """

    task_name: str
    labels: Dict[str, str]
    # order is important here, otherwise pydantic will always choose str.
    # we use int instead of Literal[-1] because pydantic thinks that -1 is always str.
    param_name: Union[Optional[int], str]
    additional_kwargs: Dict[str, Any]

    def dumps(self) -> str:
        """
        Dumps step as string.

        :return: returns json.
        """
        return self.model_dump_json()

    @classmethod
    def loads(cls, data: str) -> "SequentialStep":
        """
        Parses sequential step from string.

        :param data: dumped data.
        :return: parsed step.
        """
        return pydantic.TypeAdapter(SequentialStep).validate_json(data)

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
        Runs next task.

        This step is simple.

        It creates new task and passes the result of
        the previous task as the first argument.

        Or it may pass it as key word argument,
        if param_name is not None.

        :param broker: current broker.
        :param step_number: current step number.
        :param parent_task_id: current step's task id.
        :param task_id: new task id.
        :param pipe_data: serialized pipeline.
        :param result: result of the previous task.
        """
        kicker: "AsyncKicker[Any, Any]" = (
            AsyncKicker(
                task_name=self.task_name,
                broker=broker,
                labels=self.labels,
            )
            .with_task_id(task_id)
            .with_labels(
                **{PIPELINE_DATA: pipe_data, CURRENT_STEP: step_number},  # type: ignore
            )
        )
        if isinstance(self.param_name, str):
            self.additional_kwargs[self.param_name] = result.return_value
            await kicker.kiq(**self.additional_kwargs)
        elif self.param_name == EMPTY_PARAM_NAME:
            await kicker.kiq(**self.additional_kwargs)
        else:
            await kicker.kiq(result.return_value, **self.additional_kwargs)

    @classmethod
    def from_task(
        cls,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        param_name: Union[Optional[str], int],
        **additional_kwargs: Any,
    ) -> "SequentialStep":
        """
        Create step from given task.

        Also this method takes additional
        parameters.

        :param task: task to call.
        :param param_name: parameter name, defaults to None.
        :param additional_kwargs: additional kwargs to task.
        :return: new sequential step.
        """
        if isinstance(task, AsyncTaskiqDecoratedTask):
            kicker = task.kicker()
        else:
            kicker = task
        message = kicker._prepare_message()  # noqa: WPS437
        return SequentialStep(
            task_name=message.task_name,
            labels=message.labels,
            param_name=param_name,
            additional_kwargs=additional_kwargs,
        )
