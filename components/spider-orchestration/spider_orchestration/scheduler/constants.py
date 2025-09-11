from __future__ import annotations

from enum import auto, IntEnum

TASK_QUEUE_LOWEST_PRIORITY = 1
TASK_QUEUE_HIGHEST_PRIORITY = 3


class SchedulerType:
    COMPRESSION = "compression"
    QUERY = "query"


class StatusIntEnum(IntEnum):
    """
    Delegates __str__ to int.__str__, matching the behavior of IntEnum in Python 3.11+.
    TODO: Remove this when our minimum supported Python version is 3.11+.
    """

    def __str__(self) -> str:
        return str(self.value)

    def to_str(self) -> str:
        return self.name


class CompressionJobStatus(StatusIntEnum):
    PENDING = 0
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    KILLED = auto()


class CompressionJobCompletionStatus(StatusIntEnum):
    SUCCEEDED = 0
    FAILED = auto()


class CompressionTaskStatus(StatusIntEnum):
    PENDING = 0
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    KILLED = auto()
