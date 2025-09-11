import asyncio
import datetime
import uuid
from abc import ABC, abstractmethod
from enum import auto, Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, validator
from spider_orchestration.scheduler.constants import (
    CompressionTaskStatus,
)
from spider_py import Job as SpiderJob


class CompressionJob(BaseModel):
    id: int
    start_time: datetime.datetime
    spider_job: SpiderJob


class CompressionTaskResult(BaseModel):
    task_id: int
    status: int
    duration: float
    error_message: Optional[str]

    @validator("status")
    def valid_status(cls, field):
        supported_status = [CompressionTaskStatus.SUCCEEDED, CompressionTaskStatus.FAILED]
        if field not in supported_status:
            raise ValueError(f'must be one of the following {"|".join(supported_status)}')
        return field


class InternalJobState(Enum):
    WAITING_FOR_REDUCER = auto()
    WAITING_FOR_DISPATCH = auto()
    RUNNING = auto()
