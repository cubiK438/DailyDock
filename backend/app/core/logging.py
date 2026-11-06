"""Centralized, lightweight logging system with sensitive data sanitization."""

import logging
import re
from typing import Optional

# Common sensitive parameter patterns to mask in log messages
SENSITIVE_PATTERNS = [
    re.compile(r"(password['\"]?\s*[:=]\s*['\"]?)([^'\"\s&,]+)", re.IGNORECASE),
    re.compile(r"(token['\"]?\s*[:=]\s*['\"]?)([^'\"\s&,]+)", re.IGNORECASE),
    re.compile(r"(secret['\"]?\s*[:=]\s*['\"]?)([^'\"\s&,]+)", re.IGNORECASE),
    re.compile(r"(api[_-]?key['\"]?\s*[:=]\s*['\"]?)([^'\"\s&,]+)", re.IGNORECASE),
    re.compile(r"(authorization['\"]?\s*[:=]\s*['\"]?Bearer\s+)([^'\"\s&,]+)", re.IGNORECASE),
]


class SanitizingFormatter(logging.Formatter):
    """Logging formatter that redacts detected secrets and credentials."""

    def format(self, record: logging.LogRecord) -> str:
        original = super().format(record)
        sanitized = original
        for pattern in SENSITIVE_PATTERNS:
            sanitized = pattern.sub(r"\1********", sanitized)
        return sanitized


def setup_logging(log_level: str = "INFO") -> None:
    """Configure standardized application logging for DailyDock.

    Args:
        log_level: Severity string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure root dailydock logger
    logger = logging.getLogger("dailydock")
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates on re-configuration
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    formatter = SanitizingFormatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Prevent propagation to root logger to avoid duplicate log outputs
    logger.propagate = False


def get_logger(module_name: str) -> logging.Logger:
    """Obtain a namespaced child logger for an application module.

    Args:
        module_name: Name of the module requesting the logger.

    Returns:
        A logging.Logger instance prefixed with 'dailydock.'.
    """
    if module_name.startswith("dailydock."):
        return logging.getLogger(module_name)
    if module_name == "dailydock":
        return logging.getLogger("dailydock")
    return logging.getLogger(f"dailydock.{module_name}")

