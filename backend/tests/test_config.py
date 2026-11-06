"""Unit tests for centralized configuration management."""

import os
from pathlib import Path
import pytest
from app.config import Settings, _load_env_file
from app.core.errors import ConfigurationError


def test_default_configuration_is_valid():
    """Default Settings instance should pass validation without error."""
    settings = Settings()
    settings.validate()
    assert settings.app_name == "DailyDock"
    assert settings.env in {"development", "production", "test"}
    assert 1 <= settings.port <= 65535
    assert settings.task_max_concurrency >= 1
    assert settings.task_max_pending >= 1
    assert settings.event_max_concurrency >= 1
    assert settings.shutdown_grace_period_seconds >= 0


def test_invalid_environment_raises_configuration_error():
    """Specifying an unknown environment must raise ConfigurationError."""
    settings = Settings(env="invalid_env")
    with pytest.raises(ConfigurationError) as exc_info:
        settings.validate()
    assert "Invalid environment" in str(exc_info.value)
    assert exc_info.value.module == "config"


def test_invalid_port_raises_configuration_error():
    """Port numbers outside 1..65535 must raise ConfigurationError."""
    with pytest.raises(ConfigurationError):
        Settings(port=0).validate()

    with pytest.raises(ConfigurationError):
        Settings(port=70000).validate()


def test_invalid_log_level_raises_configuration_error():
    """Unsupported log levels must raise ConfigurationError."""
    with pytest.raises(ConfigurationError):
        Settings(log_level="VERBOSE").validate()


def test_invalid_concurrency_and_queue_limits():
    """Concurrency and queue limits below 1 must raise ConfigurationError."""
    with pytest.raises(ConfigurationError):
        Settings(task_max_concurrency=0).validate()

    with pytest.raises(ConfigurationError):
        Settings(task_max_pending=0).validate()

    with pytest.raises(ConfigurationError):
        Settings(event_max_concurrency=0).validate()

    with pytest.raises(ConfigurationError):
        Settings(shutdown_grace_period_seconds=-1.0).validate()


def test_sanitized_dict_masks_sensitive_keys():
    """get_sanitized_dict should mask keys containing secret/password/token/key."""
    settings = Settings()
    setattr(settings, "api_secret_key", "supersecret123")
    setattr(settings, "db_password", "mypassword")

    sanitized = settings.get_sanitized_dict()
    assert sanitized["api_secret_key"] == "********"
    assert sanitized["db_password"] == "********"
    assert sanitized["port"] == settings.port


def test_load_env_file_parses_correctly(tmp_path: Path):
    """_load_env_file should parse key=value lines, ignoring comments and stripping quotes."""
    test_env = tmp_path / "test.env"
    test_env.write_text(
        "# Comment line\n"
        "TEST_DD_KEY1=value1\n"
        "TEST_DD_KEY2='single_quoted'\n"
        "TEST_DD_KEY3=\"double_quoted\"\n"
        "TEST_DD_EMPTY=\n"
        "\n"
    )

    _load_env_file(test_env)
    assert os.environ.get("TEST_DD_KEY1") == "value1"
    assert os.environ.get("TEST_DD_KEY2") == "single_quoted"
    assert os.environ.get("TEST_DD_KEY3") == "double_quoted"
    assert os.environ.get("TEST_DD_EMPTY") == ""

