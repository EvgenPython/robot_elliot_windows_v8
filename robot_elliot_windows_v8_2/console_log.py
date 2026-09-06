"""UTF-8 console tee implemented inside Python (PowerShell 5 safe)."""

from __future__ import annotations

import atexit
import os
import sys
from pathlib import Path


class _Tee:
    def __init__(self, console, log_file):
        self.console = console
        self.log_file = log_file

    def write(self, value):
        self.console.write(value)
        self.log_file.write(value)
        return len(value)

    def flush(self):
        self.console.flush()
        self.log_file.flush()

    def isatty(self):
        return bool(getattr(self.console, "isatty", lambda: False)())

    @property
    def encoding(self):
        return getattr(self.console, "encoding", "utf-8")


def install_console_log() -> Path | None:
    raw_path = os.getenv("ROBOT_LOG_FILE", "").strip()
    if not raw_path:
        return None
    path = Path(raw_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    log_file = path.open("a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    atexit.register(log_file.close)
    return path
