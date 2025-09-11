from __future__ import annotations

import typing
from enum import auto

from clp_py_utils.clp_config import S3Config
from pydantic import BaseModel, validator
from strenum import LowercaseStrEnum


class InputType(LowercaseStrEnum):
    FS = auto()
    S3 = auto()


class PathsToCompress(BaseModel):
    file_paths: typing.List[str]
    group_ids: typing.List[int]
    st_sizes: typing.List[int]
    empty_directories: typing.List[str] = None


class FsInputConfig(BaseModel):
    type: typing.Literal[InputType.FS.value] = InputType.FS.value
    dataset: typing.Optional[str] = None
    paths_to_compress: typing.List[str]
    path_prefix_to_remove: str = None
    timestamp_key: typing.Optional[str] = None


class S3InputConfig(S3Config):
    type: typing.Literal[InputType.S3.value] = InputType.S3.value
    dataset: typing.Optional[str] = None
    timestamp_key: typing.Optional[str] = None


class OutputConfig(BaseModel):
    tags: typing.Optional[typing.List[str]] = None
    target_archive_size: int
    target_dictionaries_size: int
    target_segment_size: int
    target_encoded_file_size: int
    compression_level: int


class ClpIoConfig(BaseModel):
    input: typing.Union[FsInputConfig, S3InputConfig]
    output: OutputConfig
