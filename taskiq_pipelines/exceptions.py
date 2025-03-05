from typing import ClassVar, Union

from taskiq import TaskiqError


class PipelineError(TaskiqError):
    """Generic pipeline error."""


class StepError(PipelineError):
    """Error found while mapping step."""

    __template__ = (
        "Task {task_id} returned an error. {_STEP_NAME} failed. Reason: {error}"
    )
    _STEP_NAME: ClassVar[str]

    task_id: str
    error: Union[BaseException, None]


class MappingError(StepError):
    """Error found while mapping step."""

    _STEP_NAME = "mapping"


class FilterError(StepError):
    """Error found while filtering step."""

    _STEP_NAME = "filtering"


class AbortPipeline(PipelineError):  # noqa: N818
    """
    Abort curret pipeline execution.

    This error can be thrown from
    act method of a step.

    It imediately aborts current pipeline
    execution.
    """

    __template__ = "Pipeline was aborted. {reason}"

    reason: str = "No reason provided."
