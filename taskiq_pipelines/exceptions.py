from taskiq import TaskiqError


class PipelineError(TaskiqError):
    """Generic pipeline error."""


class AbortPipeline(PipelineError):
    """
    Abort curret pipeline execution.

    This error can be thrown from
    act method of a step.

    It imediately aborts current pipeline
    execution.
    """
