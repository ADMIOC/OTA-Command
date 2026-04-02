"""
OTA Command — Structured Logger
Consistent logging across all phases with phase tagging.
"""

import logging
import sys
from datetime import datetime, timezone

# Create root logger
_logger = logging.getLogger("ota_command")
_logger.setLevel(logging.DEBUG)

# Console handler with structured format
_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.DEBUG)
_formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(phase)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handler.setFormatter(_formatter)
_logger.addHandler(_handler)


class PhaseLogger:
    """Logger scoped to a specific pipeline phase."""

    def __init__(self, phase_name: str):
        self.phase = phase_name
        self.extra = {"phase": phase_name}

    def info(self, msg: str):
        _logger.info(msg, extra=self.extra)

    def warn(self, msg: str):
        _logger.warning(msg, extra=self.extra)

    def error(self, msg: str):
        _logger.error(msg, extra=self.extra)

    def debug(self, msg: str):
        _logger.debug(msg, extra=self.extra)

    def success(self, msg: str):
        _logger.info(f"✓ {msg}", extra=self.extra)

    def start(self, msg: str = "Phase started"):
        _logger.info(f"▶ {msg}", extra=self.extra)

    def complete(self, msg: str = "Phase complete"):
        _logger.info(f"✓ {msg}", extra=self.extra)


def get_logger(phase_name: str) -> PhaseLogger:
    """Get a logger scoped to a pipeline phase."""
    return PhaseLogger(phase_name)
