"""Centralized application configuration settings for DailyDock."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.errors import ConfigurationError

VALID_ENVIRONMENTS = {"development", "production", "test"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
SENSITIVE_FIELD_NAMES = {"password", "token", "secret", "key", "api_key", "credential"}

# Canonical root directory of the DailyDock repository
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def _resolve_data_dir(data_dir: Path) -> Path:
    """Resolve relative data directories from the repository root, not the current directory."""
    data_dir = data_dir.expanduser()
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    return data_dir.resolve()


def _load_env_file(dotenv_path: Optional[Path] = None) -> None:
    """Load key-value pairs from a .env file into os.environ without third-party dependencies.

    Args:
        dotenv_path: Optional explicit Path to a .env file. If omitted, searches
            the current directory and project root.
    """
    candidates = []
    if dotenv_path:
        candidates.append(dotenv_path)
    else:
        # Search current working directory, backend folder, and project root
        cwd = Path.cwd()
        candidates.extend([
            cwd / ".env",
            cwd / "backend" / ".env",
            Path(__file__).resolve().parent.parent / ".env",
            PROJECT_ROOT / ".env",
        ])

    target_file = None
    for candidate in candidates:
        if candidate.is_file():
            target_file = candidate
            break

    if not target_file:
        return

    try:
        with open(target_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, val = line.partition("=")
                    key = key.strip()
                    val = val.strip()
                    # Strip surrounding matching quotes
                    if len(val) >= 2 and (
                        (val.startswith('"') and val.endswith('"'))
                        or (val.startswith("'") and val.endswith("'"))
                    ):
                        val = val[1:-1]
                    # Do not overwrite already set environment variables
                    if key and key not in os.environ:
                        os.environ[key] = val
    except Exception:
        # If .env reading fails, proceed with system environment variables
        pass


# Load environment variables on module import
_load_env_file()


@dataclass
class Settings:
    """Centralized DailyDock backend configuration loaded from environment variables."""

    app_name: str = "DailyDock"
    app_version: str = "0.1.0"
    env: str = field(
        default_factory=lambda: os.getenv("DAILYDOCK_ENV", "development").lower()
    )
    host: str = field(
        default_factory=lambda: os.getenv("DAILYDOCK_HOST", "0.0.0.0")
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_PORT", "8000"))
    )
    cors_origins: List[str] = field(
        default_factory=lambda: [
            origin.strip()
            for origin in os.getenv(
                "DAILYDOCK_CORS_ORIGINS",
                "http://localhost:5173,http://127.0.0.1:5173",
            ).split(",")
            if origin.strip()
        ]
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("DAILYDOCK_LOG_LEVEL", "INFO").upper()
    )
    shutdown_grace_period_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("DAILYDOCK_SHUTDOWN_GRACE_PERIOD", "5.0")
        )
    )
    task_max_concurrency: int = field(
        default_factory=lambda: int(
            os.getenv("DAILYDOCK_TASK_MAX_CONCURRENCY", "5")
        )
    )
    task_max_pending: int = field(
        default_factory=lambda: int(
            os.getenv("DAILYDOCK_TASK_MAX_PENDING", "20")
        )
    )
    event_max_concurrency: int = field(
        default_factory=lambda: int(
            os.getenv("DAILYDOCK_EVENT_MAX_CONCURRENCY", "10")
        )
    )

    # Database and Persistent Storage Configuration
    data_dir: Path = field(
        default_factory=lambda: _resolve_data_dir(
            Path(os.getenv("DAILYDOCK_DATA_DIR", str(DEFAULT_DATA_DIR)))
        )
    )
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DAILYDOCK_DATABASE_URL",
            f"sqlite+aiosqlite:///{_resolve_data_dir(Path(os.getenv('DAILYDOCK_DATA_DIR', str(DEFAULT_DATA_DIR)))) / 'dailydock.db'}",
        )
    )
    database_echo: bool = field(
        default_factory=lambda: os.getenv(
            "DAILYDOCK_DATABASE_ECHO", "false"
        ).lower()
        in ("true", "1", "yes")
    )
    database_busy_timeout: int = field(
        default_factory=lambda: int(
            os.getenv("DAILYDOCK_DATABASE_BUSY_TIMEOUT", "5000")
        )
    )
    database_wal_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "DAILYDOCK_DATABASE_WAL_ENABLED", "true"
        ).lower()
        in ("true", "1", "yes")
    )

    def __post_init__(self) -> None:
        self.data_dir = _resolve_data_dir(self.data_dir)

    def get_resolved_database_url(self) -> str:
        """Resolve database URL ensuring relative SQLite paths anchor to canonical data_dir."""
        url = self.database_url
        sqlite_prefix = "sqlite+aiosqlite:///"
        if url.startswith(sqlite_prefix):
            db_path_str = url[len(sqlite_prefix) :]
            # If in-memory database, return directly
            if db_path_str == ":memory:":
                return url
            db_path = Path(db_path_str)
            if not db_path.is_absolute():
                # Anchor relative path to canonical data_dir
                resolved = (self.data_dir / db_path.name).resolve()
                return f"{sqlite_prefix}{resolved}"
        return url

    def validate(self) -> None:
        """Validate core configuration parameters.

        Raises:
            ConfigurationError: If any configuration parameter is invalid.
        """
        if self.env not in VALID_ENVIRONMENTS:
            raise ConfigurationError(
                f"Invalid environment '{self.env}'. Allowed: {sorted(VALID_ENVIRONMENTS)}",
                module="config",
                context={"env": self.env},
            )

        if not (1 <= self.port <= 65535):
            raise ConfigurationError(
                f"Invalid port {self.port}. Must be between 1 and 65535.",
                module="config",
                context={"port": self.port},
            )

        if self.log_level not in VALID_LOG_LEVELS:
            raise ConfigurationError(
                f"Invalid log level '{self.log_level}'. Allowed: {sorted(VALID_LOG_LEVELS)}",
                module="config",
                context={"log_level": self.log_level},
            )

        if self.shutdown_grace_period_seconds < 0:
            raise ConfigurationError(
                f"Invalid shutdown_grace_period_seconds {self.shutdown_grace_period_seconds}. Must be >= 0.",
                module="config",
                context={"shutdown_grace_period_seconds": self.shutdown_grace_period_seconds},
            )

        if self.task_max_concurrency < 1:
            raise ConfigurationError(
                f"Invalid task_max_concurrency {self.task_max_concurrency}. Must be >= 1.",
                module="config",
                context={"task_max_concurrency": self.task_max_concurrency},
            )

        if self.task_max_pending < 1:
            raise ConfigurationError(
                f"Invalid task_max_pending {self.task_max_pending}. Must be >= 1.",
                module="config",
                context={"task_max_pending": self.task_max_pending},
            )

        if self.event_max_concurrency < 1:
            raise ConfigurationError(
                f"Invalid event_max_concurrency {self.event_max_concurrency}. Must be >= 1.",
                module="config",
                context={"event_max_concurrency": self.event_max_concurrency},
            )

        if not self.database_url:
            raise ConfigurationError(
                "database_url cannot be empty.",
                module="config",
                context={"database_url": self.database_url},
            )

        if self.database_busy_timeout < 0:
            raise ConfigurationError(
                f"Invalid database_busy_timeout {self.database_busy_timeout}. Must be >= 0.",
                module="config",
                context={"database_busy_timeout": self.database_busy_timeout},
            )

    def get_sanitized_dict(self) -> Dict[str, Any]:
        """Return a dictionary of settings with sensitive fields masked."""
        result = {}
        for k, v in self.__dict__.items():
            if any(sensitive in k.lower() for sensitive in SENSITIVE_FIELD_NAMES):
                result[k] = "********"
            elif isinstance(v, Path):
                result[k] = str(v)
            else:
                result[k] = v
        return result


settings = Settings()
