from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from taskiq import AsyncBroker, TaskiqResult
from typing_extensions import ClassVar


class AbstractStep(ABC):
    """Abstract pipeline step."""

    _step_name: str
    _known_steps: ClassVar[Dict[str, Type["AbstractStep"]]] = {}

    def __init_subclass__(cls, step_name: str, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Sets step name to the step.
        cls._step_name = step_name
        # Registers new subclass in the dict of
        # known steps.
        cls._known_steps[step_name] = cls

    @abstractmethod
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
        Perform pipeline action.

        If you create task, please
        assign given task_id to this task,
        it helps clients to identify currently
        executed task.

        :param broker: current broker.
        :param step_number: current step number.
        :param parent_task_id: current task id.
        :param task_id: task_id to use.
        :param pipe_data: serialized pipeline must be in labels.
        :param result: result of a previous task.
        """
