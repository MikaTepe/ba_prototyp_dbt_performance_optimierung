from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class ContainerConfig:
    name: str
    description: str = ""


@dataclass(frozen=True)
class OAuthConfig:
    enabled: bool = False
    open_mode: str = "none"  # none | host | container
    open_first_url_only: bool = True


@dataclass(frozen=True)
class ExecutionConfig:
    parallel: bool = False
    max_parallel_jobs: int = 1


@dataclass(frozen=True)
class WorkflowConfig:
    podman_binary: str
    project_dir_in_container: str
    profiles_dir_in_container: str
    log_dir: Path
    oauth: OAuthConfig
    execution: ExecutionConfig
    containers: dict[str, ContainerConfig]


@dataclass(frozen=True)
class DbtRunRequest:
    run_id: str
    container_name: str
    dbt_command: str
    model_name: Optional[str]
    vars: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CommandResult:
    run_id: str
    command: list[str]
    returncode: int
    duration_seconds: float
    log_path: Path

    @property
    def successful(self) -> bool:
        return self.returncode == 0


@dataclass(frozen=True)
class BatchResult:
    results: list[CommandResult]

    @property
    def successful(self) -> bool:
        return all(result.successful for result in self.results)

    @property
    def failed_results(self) -> list[CommandResult]:
        return [result for result in self.results if not result.successful]