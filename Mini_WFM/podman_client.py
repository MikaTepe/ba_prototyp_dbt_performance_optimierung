from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Optional

from domain import CommandResult
from oauth import OAuthUrlHandler


class PodmanExecutionError(Exception):
    pass


class PodmanClient:
    def __init__(self, podman_binary: str) -> None:
        self.podman_binary = podman_binary

    def assert_container_reachable(self, container_name: str) -> None:
        command = [
            self.podman_binary,
            "exec",
            container_name,
            "sh",
            "-lc",
            "echo container_ready",
        ]

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if result.returncode != 0:
            raise PodmanExecutionError(
                f"Container is not reachable: {container_name}\n"
                f"stdout={result.stdout}\n"
                f"stderr={result.stderr}"
            )

        if "container_ready" not in result.stdout:
            raise PodmanExecutionError(
                f"Unexpected container readiness response from {container_name}: "
                f"{result.stdout}"
            )

    def run_streamed(
        self,
        run_id: str,
        command: list[str],
        log_path: Path,
        oauth_handler: Optional[OAuthUrlHandler] = None,
    ) -> CommandResult:
        start = time.perf_counter()

        with log_path.open("w", encoding="utf-8") as log:
            log.write("RUN ID:\n")
            log.write(run_id)
            log.write("\n\n")

            log.write("COMMAND:\n")
            log.write(" ".join(command))
            log.write("\n\n")
            log.flush()

            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            assert process.stdout is not None

            for line in process.stdout:
                print(f"[{run_id}] {line}", end="")
                log.write(line)
                log.flush()

                if oauth_handler is not None:
                    oauth_handler.handle_line(line)

            returncode = process.wait()

        duration = time.perf_counter() - start

        return CommandResult(
            run_id=run_id,
            command=command,
            returncode=returncode,
            duration_seconds=duration,
            log_path=log_path,
        )