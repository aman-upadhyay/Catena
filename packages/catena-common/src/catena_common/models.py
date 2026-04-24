"""Shared data models and validators for Catena."""

from __future__ import annotations

import re
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

JOB_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
SAFE_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")

ExtraValue = str | int | float | bool | None | list[str]


def is_safe_job_id(value: str) -> bool:
    """Return True when a job id uses only safe characters."""

    return bool(JOB_ID_PATTERN.fullmatch(value))


def is_relative_path(value: str) -> bool:
    """Return True when a path is relative in both POSIX and Windows forms."""

    return bool(value) and not PurePosixPath(value).is_absolute() and not PureWindowsPath(value).is_absolute()


def has_parent_reference(value: str) -> bool:
    """Return True when a path contains parent directory traversal."""

    return ".." in PurePosixPath(value).parts


def is_safe_relative_name(value: str) -> bool:
    """Return True when a file name is relative and each path segment is safe."""

    if not is_relative_path(value) or has_parent_reference(value):
        return False

    parts = PurePosixPath(value).parts
    if not parts:
        return False

    for part in parts:
        if part in {"", "."} or not SAFE_PATH_SEGMENT_PATTERN.fullmatch(part):
            return False

    return True


def validate_job_id(value: str) -> str:
    """Validate and return a safe job id."""

    if not is_safe_job_id(value):
        msg = "job_id must contain only letters, numbers, underscores, and dashes"
        raise ValueError(msg)
    return value


def validate_relative_path(value: str) -> str:
    """Validate and return a relative path."""

    if not is_relative_path(value):
        msg = "path must be relative and not absolute"
        raise ValueError(msg)
    return value


def validate_safe_relative_name(value: str) -> str:
    """Validate and return a safe relative file name."""

    if not is_safe_relative_name(value):
        msg = "file name must be a safe relative path and must not contain '..'"
        raise ValueError(msg)
    return value


class CatenaModel(BaseModel):
    """Base model with JSON helpers shared across Catena schemas."""

    model_config = ConfigDict(extra="forbid")

    def to_json(self, **kwargs: Any) -> str:
        """Serialize this model to a JSON string."""

        return self.model_dump_json(**kwargs)

    @classmethod
    def from_json(cls, data: str | bytes | bytearray, **kwargs: Any) -> Self:
        """Deserialize a model instance from JSON."""

        return cls.model_validate_json(data, **kwargs)


class TaskType(str, Enum):
    """Supported task categories."""

    MG5_PYTHIA = "mg5_pythia"
    DELPHES = "delphes"
    PYTHIA8 = "pythia8"
    SHERPA = "sherpa"
    CPP = "cpp"
    PYTHON = "python"


class JobState(str, Enum):
    """Lifecycle states for a Catena job."""

    RECEIVED = "RECEIVED"
    PREPARING = "PREPARING"
    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class InputFile(CatenaModel):
    """File payload supplied with a job request."""

    name: str
    mode: Literal["inline", "uploaded", "server_path"] = "inline"
    content_b64: str | None = None
    path: str | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Ensure an input file name is a safe relative path."""

        return validate_safe_relative_name(value)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str | None) -> str | None:
        """Ensure any server-side path is absolute when provided."""

        if value is None:
            return value
        if not PurePosixPath(value).is_absolute() and not PureWindowsPath(value).is_absolute():
            msg = "server_path inputs require an absolute path"
            raise ValueError(msg)
        return value

    @model_validator(mode="before")
    @classmethod
    def apply_inline_compatibility(cls, data: Any) -> Any:
        """Preserve legacy inline-file payloads when mode is omitted."""

        if not isinstance(data, dict):
            return data
        if "mode" not in data and data.get("content_b64") is not None:
            data = dict(data)
            data["mode"] = "inline"
        return data

    @model_validator(mode="after")
    def validate_mode_fields(self) -> Self:
        """Validate the field requirements for each input mode."""

        if self.mode == "inline":
            if not self.content_b64:
                msg = "inline inputs require content_b64"
                raise ValueError(msg)
            if self.path is not None:
                msg = "inline inputs must not set path"
                raise ValueError(msg)
        elif self.mode == "uploaded":
            if self.content_b64 is not None or self.path is not None:
                msg = "uploaded inputs require only name"
                raise ValueError(msg)
        elif self.mode == "server_path":
            if not self.path:
                msg = "server_path inputs require path"
                raise ValueError(msg)
            if self.content_b64 is not None:
                msg = "server_path inputs must not set content_b64"
                raise ValueError(msg)
        return self


class JobRequest(CatenaModel):
    """Client-submitted request to create a Catena job."""

    job_id: str
    task_type: TaskType
    entry_file: str | None = None
    cli_args: list[str] = Field(default_factory=list)
    input_files: list[InputFile] = Field(default_factory=list)
    extra: dict[str, ExtraValue] = Field(default_factory=dict)

    @field_validator("job_id")
    @classmethod
    def validate_job_id_field(cls, value: str) -> str:
        """Ensure the request job id is safe."""

        return validate_job_id(value)

    @field_validator("entry_file")
    @classmethod
    def validate_entry_file(cls, value: str | None) -> str | None:
        """Ensure the entry file, when present, is not absolute."""

        if value is None:
            return value
        return validate_relative_path(value)


class JobStatus(CatenaModel):
    """Status view for an existing Catena job."""

    job_id: str
    state: JobState
    active: bool
    slurm_job_id: str | None = None
    job_dir: str
    message: str | None = None
    failure_reason: str | None = None
    exit_code: int | None = None

    @field_validator("job_id")
    @classmethod
    def validate_job_id_field(cls, value: str) -> str:
        """Ensure the status job id is safe."""

        return validate_job_id(value)


class SlurmSettings(CatenaModel):
    """Configured SLURM settings."""

    partition: str
    requeue: bool
    nodes: int
    ntasks: int
    cpus_per_task: int
    mem_mb: int
    time: str
    array: str
