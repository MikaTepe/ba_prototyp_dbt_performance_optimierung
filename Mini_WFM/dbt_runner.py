from __future__ import annotations

import json

from domain import CommandResult, DbtRunRequest, WorkflowConfig
from logging_utils import build_log_path
from oauth import OAuthUrlHandler
from podman_client import PodmanClient


class DbtRunner:
    def __init__(self, config: WorkflowConfig, podman_client: PodmanClient) -> None:
        self.config = config
        self.podman_client = podman_client

    def run(self, request: DbtRunRequest) -> CommandResult:
        self._validate_request(request)

        self.podman_client.assert_container_reachable(request.container_name)

        command = self._build_podman_dbt_command(request)

        log_path = build_log_path(
            log_dir=self.config.log_dir,
            container_name=request.container_name,
            command_name=request.run_id,
        )

        oauth_handler = OAuthUrlHandler(
            enabled=self.config.oauth.enabled,
            open_mode=self.config.oauth.open_mode,
            open_first_url_only=self.config.oauth.open_first_url_only,
            podman_binary=self.config.podman_binary,
            container_name=request.container_name,
        )

        return self.podman_client.run_streamed(
            run_id=request.run_id,
            command=command,
            log_path=log_path,
            oauth_handler=oauth_handler,
        )

    def _validate_request(self, request: DbtRunRequest) -> None:
        if request.container_name not in self.config.containers:
            known = ", ".join(sorted(self.config.containers.keys()))
            raise ValueError(
                f"Unknown container '{request.container_name}'. "
                f"Known containers: {known}"
            )

        if request.dbt_command == "snapshot" and not request.model_name:
            raise ValueError("model_name is required for dbt snapshot runs.")

    def _build_podman_dbt_command(self, request: DbtRunRequest) -> list[str]:
        base = [
            self.config.podman_binary,
            "exec",
            "-w",
            self.config.project_dir_in_container,
            "-e",
            f"DBT_PROFILES_DIR={self.config.profiles_dir_in_container}",
            "-e",
            "DBT_SEND_ANONYMOUS_USAGE_STATS=false",
            request.container_name,
            "dbt",
        ]

        dbt_global_args = [
            "--log-path",
            f"logs/workflow_manager/{request.run_id}",
        ]

        if request.dbt_command == "debug":
            return base + dbt_global_args + ["debug"]

        if request.dbt_command == "snapshot":
            dbt_vars = json.dumps(request.vars, sort_keys=True)

            return (
                base
                + dbt_global_args
                + [
                    "snapshot",
                    "--select",
                    request.model_name,
                    "--vars",
                    dbt_vars,
                ]
            )

        if request.dbt_command == "run":
            dbt_vars = json.dumps(request.vars, sort_keys=True)

            command = base + dbt_global_args + ["run"]

            if request.model_name:
                command += ["--select", request.model_name]

            command += ["--vars", dbt_vars]
            return command

        raise ValueError(f"Unsupported dbt command: {request.dbt_command}")