"""Unit tests for centralized logging system and secret redaction."""

import io
import logging
from app.core.logging import SanitizingFormatter, get_logger, setup_logging


def test_get_logger_namespacing():
    """get_logger should prefix module names with 'dailydock.'."""
    logger = get_logger("storage")
    assert logger.name == "dailydock.storage"

    # Preserves existing prefix
    logger2 = get_logger("dailydock.tasks")
    assert logger2.name == "dailydock.tasks"


def test_sanitizing_formatter_masks_passwords_and_tokens():
    """SanitizingFormatter should redact sensitive strings in log outputs."""
    formatter = SanitizingFormatter(fmt="%(message)s")

    record1 = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Connecting with password='mySuperSecretPassword' to host",
        args=(),
        exc_info=None,
    )
    formatted1 = formatter.format(record1)
    assert "mySuperSecretPassword" not in formatted1
    assert "password='********'" in formatted1

    record2 = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Using api_key: abc123def456 for service",
        args=(),
        exc_info=None,
    )
    formatted2 = formatter.format(record2)
    assert "abc123def456" not in formatted2
    assert "api_key: ********" in formatted2


def test_setup_logging_configures_stream_handler():
    """setup_logging should configure the dailydock root logger."""
    setup_logging("DEBUG")
    root_logger = logging.getLogger("dailydock")
    assert root_logger.level == logging.DEBUG
    assert len(root_logger.handlers) >= 1

