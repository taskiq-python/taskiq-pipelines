import json
from typing import Any, Generic, List, Optional, TypeVar, Union

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqTask
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from typing_extensions import ParamSpec

from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA
from taskiq_pipelines.steps import MapperStep, SequentialStep, parse_step

_ReturnType = TypeVar("_ReturnType")
_FuncParams = ParamSpec("_FuncParams")
_T2 = TypeVar("_T2")


class DumpedStep(pydantic.BaseModel):
    """Dumped state model."""

    step_type: str
    step_data: str
    task_id: str


class Pipeline(Generic[_FuncParams, _ReturnType]):
    """
    Pipeline constructor.

    This class helps you to build pipelines.
    It creates all needed data and manages
    task ids. Also it has helper methods,
    to easily add new pipeline steps.

    Of course it can be done manually,
    but it's nice to have.
    """

    def __init__(  # noqa: WPS234
        self,
        broker: AsyncBroker,
        task: Optional[
            Union[
                AsyncKicker[_FuncParams, _ReturnType],
                AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
            ]
        ] = None,
    ) -> None:
        self.broker = broker
        self.steps: "List[DumpedStep]" = []
        if task:
            self.call_next(task)

    def call_next(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, _T2],
            AsyncTaskiqDecoratedTask[Any, _T2],
        ],
        param_name: Optional[str] = None,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _T2]":
        """
        Adds sequential step.

        This task will be executed right after
        the previous and result of the previous task
        will be passed as the first argument,
        or it will be passed as key word argument,
        if param_name is specified.

        :param task: task to execute.
        :param param_name: kwarg param name, defaults to None
        :param additional_kwargs: additional kwargs to task.
        :return: updated pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=SequentialStep.step_name,
                step_data=SequentialStep.from_task(
                    task=task,
                    param_name=param_name,
                    **additional_kwargs,
                ).dumps(),
                task_id="",
            ),
        )
        return self  # type: ignore

    def map(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, _T2],
            AsyncTaskiqDecoratedTask[Any, _T2],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, List[_T2]]":
        """
        Create new map task.

        This task is used to map values of an
        iterable.

        It creates many subtasks and then collects
        all results.

        :param task: task to execute on each value of an iterable.
        :param param_name: param name to use to inject the result of
            the previous task. If none, result injected as the first argument.
        :param skip_errors: skip error results, defaults to False.
        :param check_interval: how often task completion is checked.
        :param additional_kwargs: additional function's kwargs.
        :return: pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=MapperStep.step_name,
                step_data=MapperStep.from_task(
                    task=task,
                    param_name=param_name,
                    skip_errors=skip_errors,
                    check_interval=check_interval,
                    **additional_kwargs,
                ).dumps(),
                task_id="",
            ),
        )
        return self  # type: ignore

    def dumps(self) -> str:
        """
        Dumps current pipeline as string.

        :returns: serialized pipeline.
        """
        return json.dumps(
            [step.dict() for step in self.steps],
        )

    @classmethod
    def loads(cls, broker: AsyncBroker, pipe_data: str) -> "Pipeline[Any, Any]":
        """
        Parses serialized pipeline.

        This method requires broker,
        to make pipeline kickable.

        :param broker: broker to use when call kiq.
        :param pipe_data: serialized pipeline data.
        :return: new
        """
        pipe: "Pipeline[Any, Any]" = Pipeline(broker)
        pipe.steps = pydantic.parse_raw_as(List[DumpedStep], pipe_data)
        return pipe

    async def kiq(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """
        Kiq pipeline.

        This function is used as kiq in functions,
        but it saves current pipeline as
        custom label, so worker can understand,
        what to do next.

        :param args: first function's args.
        :param kwargs: first function's kwargs.

        :raises ValueError: if pipe is empty, or
            first step isn't sequential.

        :return: TaskqTask for the final function.
        """
        if not self.steps:
            raise ValueError("Pipeline is empty.")
        self._update_task_ids()
        step = self.steps[0]
        parsed_step = parse_step(step.step_type, step.step_data)
        if not isinstance(parsed_step, SequentialStep):
            raise ValueError("First step must be sequential.")
        kicker = (
            AsyncKicker(
                parsed_step.task_name,
                broker=self.broker,
                labels=parsed_step.labels,
            )
            .with_task_id(step.task_id)
            .with_labels(
                **{CURRENT_STEP: 0, PIPELINE_DATA: self.dumps()},  # type: ignore
            )
        )
        taskiq_task = await kicker.kiq(*args, **kwargs)
        taskiq_task.task_id = self.steps[-1].task_id
        return taskiq_task

    def _update_task_ids(self) -> None:
        """Calculates task ids for each step in the pipeline."""
        for step in self.steps:
            step.task_id = self.broker.id_generator()