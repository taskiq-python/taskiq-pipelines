from types import CoroutineType
from typing import (
    Any,
    Coroutine,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TypeVar,
    Union,
    overload,
)

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqTask, TaskiqResult
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from typing_extensions import ParamSpec

from taskiq_pipelines.constants import CURRENT_STEP, EMPTY_PARAM_NAME, PIPELINE_DATA
from taskiq_pipelines.steps import FilterStep, MapperStep, SequentialStep, parse_step
from taskiq_pipelines.steps.group import GroupStep
from taskiq_pipelines.task_group import Group

_ReturnType = TypeVar("_ReturnType")
_FuncParams = ParamSpec("_FuncParams")
_T2 = TypeVar("_T2")


class DumpedStep(pydantic.BaseModel):
    """Dumped state model."""

    step_type: str
    step_data: Dict[str, Any]
    task_id: str


DumpedSteps = pydantic.RootModel[List[DumpedStep]]


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

    @overload
    def __init__(
        self: "Pipeline[[], _ReturnType]",
        broker: AsyncBroker,
        task: Optional[Group[_ReturnType]] = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        broker: AsyncBroker,
        task: Optional[
            Union[
                AsyncKicker[_FuncParams, _ReturnType],
                AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
            ]
        ] = None,
    ) -> None: ...

    def __init__(
        self,
        broker: AsyncBroker,
        task: Optional[
            Union[
                AsyncKicker[_FuncParams, _ReturnType],
                AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
                Group[_ReturnType],
            ]
        ] = None,
    ) -> None:
        self.broker = broker
        self.steps: "List[DumpedStep]" = []
        if not task:
            return
        if isinstance(task, Group):
            self.group(task)
            return
        self.call_next(task)

    @overload
    def call_next(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, Coroutine[Any, Any, _T2]],
            AsyncKicker[Any, "CoroutineType[Any, Any, _T2]"],
            AsyncTaskiqDecoratedTask[Any, Coroutine[Any, Any, _T2]],
            AsyncTaskiqDecoratedTask[Any, "CoroutineType[Any, Any, _T2]"],
        ],
        param_name: Union[Optional[str], Literal[-1]] = None,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _T2]": ...

    @overload
    def call_next(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, _T2],
            AsyncTaskiqDecoratedTask[Any, _T2],
        ],
        param_name: Union[Optional[str], Literal[-1]] = None,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _T2]": ...

    def call_next(
        self,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        param_name: Union[Optional[str], Literal[-1]] = None,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Adds sequential step.

        This task will be executed right after
        the previous and result of the previous task
        will be passed as the first argument,
        or it will be passed as key word argument,
        if param_name is specified.

        :param task: task to execute.
        :param param_name: kwarg param name, defaults to None.
            If set to -1 (EMPTY_PARAM_NAME), result is not passed.
        :param additional_kwargs: additional kwargs to task.
        :return: updated pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=SequentialStep._step_name,
                step_data=SequentialStep.from_task(
                    task=task,
                    param_name=param_name,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def call_after(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, Coroutine[Any, Any, _T2]],
            AsyncKicker[Any, "CoroutineType[Any, Any, _T2]"],
            AsyncTaskiqDecoratedTask[Any, Coroutine[Any, Any, _T2]],
            AsyncTaskiqDecoratedTask[Any, "CoroutineType[Any, Any, _T2]"],
        ],
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _T2]": ...

    @overload
    def call_after(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, _T2],
            AsyncTaskiqDecoratedTask[Any, _T2],
        ],
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _T2]": ...

    def call_after(
        self,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        **additional_kwargs: Any,
    ) -> Any:
        """
        Adds sequential step.

        This task will be executed right after
        the previous and result of the previous task
        is not passed to the next task.

        This is equivalent to call_next(task, param_name=-1).

        :param task: task to execute.
        :param additional_kwargs: additional kwargs to task.
        :return: updated pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=SequentialStep._step_name,
                step_data=SequentialStep.from_task(
                    task=task,
                    param_name=EMPTY_PARAM_NAME,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def map(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, Coroutine[Any, Any, _T2]],
            AsyncKicker[Any, "CoroutineType[Any, Any, _T2]"],
            AsyncTaskiqDecoratedTask[Any, Coroutine[Any, Any, _T2]],
            AsyncTaskiqDecoratedTask[Any, "CoroutineType[Any, Any, _T2]"],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, List[_T2]]": ...

    @overload
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
    ) -> "Pipeline[_FuncParams, List[_T2]]": ...

    def map(
        self,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Any:
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
                step_type=MapperStep._step_name,
                step_data=MapperStep.from_task(
                    task=task,
                    param_name=param_name,
                    skip_errors=skip_errors,
                    check_interval=check_interval,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def filter(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, Coroutine[Any, Any, bool]],
            AsyncKicker[Any, "CoroutineType[Any, Any, bool]"],
            AsyncTaskiqDecoratedTask[Any, Coroutine[Any, Any, bool]],
            AsyncTaskiqDecoratedTask[Any, "CoroutineType[Any, Any, bool]"],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _ReturnType]": ...

    @overload
    def filter(
        self: "Pipeline[_FuncParams, _ReturnType]",
        task: Union[
            AsyncKicker[Any, bool],
            AsyncTaskiqDecoratedTask[Any, bool],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> "Pipeline[_FuncParams, _ReturnType]": ...

    def filter(
        self,
        task: Union[
            AsyncKicker[Any, Any],
            AsyncTaskiqDecoratedTask[Any, Any],
        ],
        param_name: Optional[str] = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Add filter step.

        This step is executed on a list of items,
        like map.

        It runs many small subtasks for each item
        in sequence and if task returns true,
        the result is added to the final list.

        :param task: task to execute on every item.
        :param param_name: parameter name to pass item into, defaults to None
        :param skip_errors: skip errors if any, defaults to False
        :param check_interval: how often the result of all subtasks is checked,
             defaults to 0.5
        :param additional_kwargs: additional function's kwargs.
        :return: pipeline with filtering step.
        """
        self.steps.append(
            DumpedStep(
                step_type=FilterStep._step_name,
                step_data=FilterStep.from_task(
                    task=task,
                    param_name=param_name,
                    skip_errors=skip_errors,
                    check_interval=check_interval,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    def group(
        self: "Pipeline[_FuncParams, _ReturnType]",
        group: Group[_T2],
    ) -> "Pipeline[_FuncParams, _T2]":
        """
        Add group task execution step.

        This step will run all tasks in parallel
        and will wait for all of them to finish.

        Results of all tasks will be returned as an iterable
        where each item is a result of the task in the group
        with the same order.

        :param group: group to execute.
        """
        self.steps.append(
            DumpedStep(
                step_type=GroupStep._step_name,
                step_data=group.to_step().model_dump(),
                task_id="",
            ),
        )
        return self  # type: ignore

    def dumpb(self) -> bytes:
        """
        Dumps current pipeline as string.

        :returns: serialized pipeline.
        """
        return self.broker.serializer.dumpb(
            DumpedSteps.model_validate(self.steps).model_dump(),
        )

    @classmethod
    def loadb(cls, broker: AsyncBroker, pipe_data: bytes) -> "Pipeline[Any, Any]":
        """
        Parses serialized pipeline.

        This method requires broker,
        to make pipeline kickable.

        :param broker: broker to use when call kiq.
        :param pipe_data: serialized pipeline data.
        :return: new
        """
        pipe: "Pipeline[Any, Any]" = Pipeline(broker)
        data = broker.serializer.loadb(pipe_data)
        pipe.steps = DumpedSteps.model_validate(data)  # type: ignore[assignment]
        return pipe

    async def _kick_sequential(
        self,
        step: SequentialStep,
        task_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncTaskiqTask[_ReturnType]:
        kicker = (
            AsyncKicker(
                step.task_name,
                broker=self.broker,
                labels=step.labels,
            )
            .with_task_id(task_id)
            .with_labels(
                **{CURRENT_STEP: 0, PIPELINE_DATA: self.dumpb()},  # type: ignore
            )
        )
        return await kicker.kiq(*args, **kwargs)

    async def _kick_group(
        self,
        group: GroupStep,
        task_id: str,
    ) -> AsyncTaskiqTask[Any]:
        await group.act(
            broker=self.broker,
            task_id=task_id,
            step_number=0,
            parent_task_id="",
            pipe_data=self.dumpb(),
            result=TaskiqResult(
                is_err=False,
                return_value=None,
                execution_time=0.0,
            ),
        )
        return AsyncTaskiqTask(
            task_id=task_id,
            result_backend=self.broker.result_backend,
        )

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
        if isinstance(parsed_step, SequentialStep):
            taskiq_task = await self._kick_sequential(
                parsed_step,
                step.task_id,
                *args,
                **kwargs,
            )
        elif isinstance(parsed_step, GroupStep):
            taskiq_task = await self._kick_group(parsed_step, step.task_id)
        else:
            raise ValueError("First step must be sequential or a group.")

        taskiq_task.task_id = self.steps[-1].task_id
        return taskiq_task

    def _update_task_ids(self) -> None:
        """Calculates task ids for each step in the pipeline."""
        for step in self.steps:
            step.task_id = self.broker.id_generator()
