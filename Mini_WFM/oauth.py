from __future__ import annotations

import re
import subprocess
import webbrowser
from dataclasses import dataclass
from typing import Iterable, Optional


URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+")


@dataclass
class OAuthUrlHandler:
    enabled: bool
    open_mode: str
    open_first_url_only: bool
    podman_binary: str
    container_name: str

    def __post_init__(self) -> None:
        self._opened_urls: set[str] = set()

    def handle_line(self, line: str) -> None:
        if not self.enabled:
            return

        for url in self._extract_urls(line):
            if self.open_first_url_only and self._opened_urls:
                return

            if url in self._opened_urls:
                continue

            self._opened_urls.add(url)
            self._open_url(url)

    def _extract_urls(self, line: str) -> Iterable[str]:
        urls = URL_PATTERN.findall(line)

        # Avoid opening random documentation links if possible.
        # Starburst/dbt OAuth links are normally HTTPS links shown during auth.
        for url in urls:
            cleaned = url.rstrip(").,;]")
            yield cleaned

    def _open_url(self, url: str) -> None:
        if self.open_mode == "none":
            return

        if self.open_mode == "host":
            webbrowser.open(url)
            return

        if self.open_mode == "container":
            self._try_open_inside_container(url)
            return

        raise ValueError(f"Unsupported OAuth open mode: {self.open_mode}")

    def _try_open_inside_container(self, url: str) -> None:
        """
        Best-effort only.

        This usually only works if the container has:
        - a browser or xdg-open installed
        - DISPLAY / Wayland forwarding configured
        - access to the host GUI session

        In most dbt/Podman setups this will not work.
        """
        commands = [
            f"python3 -m webbrowser {quote_shell(url)}",
            f"xdg-open {quote_shell(url)}",
        ]

        for command in commands:
            result = subprocess.run(
                [
                    self.podman_binary,
                    "exec",
                    self.container_name,
                    "sh",
                    "-lc",
                    command,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )

            if result.returncode == 0:
                return


def quote_shell(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"