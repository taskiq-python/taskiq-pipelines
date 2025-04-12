from types import CoroutineType
from typing import Any, Coroutine, Generic, Literal, Optional, Tuple, Union, overload

from taskiq import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from typing_extensions import ParamSpec, TypeVar, TypeVarTuple, Unpack

from taskiq_pipelines.steps.group import GroupStep, GroupStepItem

_Tups = TypeVarTuple("_Tups")
_T = TypeVar("_T")
# Whether skip_errors is set to True or False
_S = TypeVar("_S", bound=bool)
_TVal = TypeVar("_TVal")
_Params = ParamSpec("_Params")


class Group(Generic[_S, _T]):
    """
    Group of tasks.

    This class gathers multiple tasks together.
    They will run in parallel

    :param skip_errors: If True, errors in one task will not affect others.
    """

    @overload
    def __init__(
        self: "Group[Literal[True], Tuple[()]]",
        skip_errors: Literal[True],
        check_interval: float = 0.1,
    ) -> None: ...

    @overload
    def __init__(
        self: "Group[Literal[False], Tuple[()]]",
        skip_errors: bool = False,
        check_interval: float = 0.1,
    ) -> None: ...

    def __init__(
        self: "Group[Any, Tuple[()]]",
        skip_errors: bool = False,
        check_interval: float = 0.1,
    ) -> None:
        self.tasks: Tuple[GroupStepItem, ...] = ()
        self.skip_errors = skip_errors
        self.check_interval = check_interval

    @overload
    def add(
        self: "Group[Literal[True], Tuple[Unpack[_Tups]]]",
        task: Union[
            AsyncKicker[_Params, Coroutine[Any, Any, _TVal]],
            AsyncKicker[_Params, "CoroutineType[Any, Any, _TVal]"],
            AsyncTaskiqDecoratedTask[_Params, Coroutine[Any, Any, _TVal]],
            AsyncTaskiqDecoratedTask[_Params, "CoroutineType[Any, Any, _TVal]"],
        ],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> "Group[_S, Tuple[Unpack[_Tups], Optional[_TVal]]]": ...

    @overload
    def add(
        self: "Group[Literal[False], Tuple[Unpack[_Tups]]]",
        task: Union[
            AsyncKicker[_Params, Coroutine[Any, Any, _TVal]],
            AsyncKicker[_Params, "CoroutineType[Any, Any, _TVal]"],
            AsyncTaskiqDecoratedTask[_Params, Coroutine[Any, Any, _TVal]],
            AsyncTaskiqDecoratedTask[_Params, "CoroutineType[Any, Any, _TVal]"],
        ],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> "Group[_S, Tuple[Unpack[_Tups], _TVal]]": ...

    @overload
    def add(
        self: "Group[Literal[True], Tuple[Unpack[_Tups]]]",
        task: Union[
            AsyncKicker[_Params, _TVal],
            AsyncTaskiqDecoratedTask[_Params, _TVal],
        ],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> "Group[_S, Tuple[Unpack[_Tups], Optional[_TVal]]]": ...

    @overload
    def add(
        self: "Group[Literal[False], Tuple[Unpack[_Tups]]]",
        task: Union[
            AsyncKicker[_Params, _TVal],
            AsyncTaskiqDecoratedTask[_Params, _TVal],
        ],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> "Group[_S, Tuple[Unpack[_Tups], _TVal]]": ...

    def add(
        self: "Group[Any, Any]",
        task: Union[AsyncKicker[_Params, Any], AsyncTaskiqDecoratedTask[_Params, Any]],
        *args: _Params.args,
        **kwargs: _Params.kwargs,
    ) -> "Any":
        """Add task to a group."""
        kicker = task.kicker() if isinstance(task, AsyncTaskiqDecoratedTask) else task
        message = kicker._prepare_message(*args, **kwargs)
        self.tasks = (
            *self.tasks,
            GroupStepItem(
                task_name=message.task_name,
                labels=message.labels,
                labels_types=message.labels_types,
                args=message.args,
                kwargs=message.kwargs,
            ),
        )
        return self

    def to_step(self) -> GroupStep:
        """Convert group definition to a step."""
        return GroupStep(
            tasks=list(self.tasks),
            skip_errors=self.skip_errors,
            check_interval=self.check_interval,
        )
