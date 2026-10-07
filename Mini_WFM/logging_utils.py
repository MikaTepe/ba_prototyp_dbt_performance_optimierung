from __future__ import annotations

from datetime import datetime
from pathlib import Path


def timestamp_for_filename() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def ensure_log_dir(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)


def build_log_path(log_dir: Path, container_name: str, command_name: str) -> Path:
    ensure_log_dir(log_dir)

    safe_command = command_name.replace(" ", "_").replace("/", "_")
    filename = f"{timestamp_for_filename()}_{container_name}_{safe_command}.log"

    return log_dir / filename