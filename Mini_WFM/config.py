from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from domain import (
    ContainerConfig,
    DbtRunRequest,
    ExecutionConfig,
    OAuthConfig,
    WorkflowConfig,
)


class ConfigError(Exception):
    pass


def load_json(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)

    if not config_path.exists():
        raise ConfigError(f"Config file does not exist: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_workflow_config(path: str | Path) -> WorkflowConfig:
    raw = load_json(path)

    oauth_raw = raw.get("oauth", {})
    execution_raw = raw.get("execution", {})

    oauth = OAuthConfig(
        enabled=bool(oauth_raw.get("enabled", False)),
        open_mode=str(oauth_raw.get("open_mode", "none")),
        open_first_url_only=bool(oauth_raw.get("open_first_url_only", True)),
    )

    execution = ExecutionConfig(
        parallel=bool(execution_raw.get("parallel", False)),
        max_parallel_jobs=int(execution_raw.get("max_parallel_jobs", 1)),
    )

    if execution.max_parallel_jobs < 1:
        raise ConfigError("execution.max_parallel_jobs must be >= 1.")

    containers = {}
    for item in raw.get("containers", []):
        container = ContainerConfig(
            name=item["name"],
            description=item.get("description", ""),
        )
        containers[container.name] = container

    if not containers:
        raise ConfigError("No containers configured.")

    return WorkflowConfig(
        podman_binary=raw.get("podman_binary", "podman"),
        project_dir_in_container=raw["project_dir_in_container"],
        profiles_dir_in_container=raw["profiles_dir_in_container"],
        log_dir=Path(raw.get("log_dir", "logs")),
        oauth=oauth,
        execution=execution,
        containers=containers,
    )


def load_run_requests(path: str | Path) -> list[DbtRunRequest]:
    raw = load_json(path)

    runs_raw = raw.get("runs")

    # Backward compatibility with v0.1 config.
    if runs_raw is None and "test_run" in raw:
        test_run = raw["test_run"]
        runs_raw = [
            {
                "run_id": test_run.get("run_id", "single_test_run"),
                "container_name": test_run["container_name"],
                "dbt_command": test_run["dbt_command"],
                "model_name": test_run.get("model_name"),
                "vars": test_run.get("vars", {}),
            }
        ]

    if not runs_raw:
        raise ConfigError("No runs configured. Expected 'runs' list in workflow_config.json.")

    requests: list[DbtRunRequest] = []

    seen_run_ids: set[str] = set()

    for index, run in enumerate(runs_raw):
        run_id = str(run.get("run_id") or f"run_{index:03d}")

        if run_id in seen_run_ids:
            raise ConfigError(f"Duplicate run_id found: {run_id}")

        seen_run_ids.add(run_id)

        requests.append(
            DbtRunRequest(
                run_id=run_id,
                container_name=run["container_name"],
                dbt_command=run["dbt_command"],
                model_name=run.get("model_name"),
                vars=run.get("vars", {}),
            )
        )

    return requests