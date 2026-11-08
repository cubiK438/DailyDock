"""Centralized application configuration settings for DailyDock."""

import os
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from app.core.errors import ConfigurationError

VALID_ENVIRONMENTS = {"development", "production", "test"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
SENSITIVE_FIELD_NAMES = {"password", "token", "secret", "key", "api_key", "credential"}
MAX_MONITOR_CONCURRENCY = 10

# Canonical root directory of the DailyDock repository
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def _resolve_data_dir(data_dir: Path) -> Path:
    """Resolve relative data directories from the repository root, not the current directory."""
    data_dir = data_dir.expanduser()
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    return data_dir.resolve()


def _load_json_list(env_name: str) -> List[Dict[str, Any]]:
    raw = os.getenv(env_name, "[]")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(
            f"{env_name} must contain a valid JSON array.",
            module="config",
            context={"setting": env_name},
        ) from exc
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ConfigurationError(
            f"{env_name} must contain a JSON array of objects.",
            module="config",
            context={"setting": env_name},
        )
    return value


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

    # External storage volume configuration
    storage_mountpoint: Path = field(
        default_factory=lambda: Path(
            os.getenv("DAILYDOCK_STORAGE_MOUNTPOINT", "/mnt/dailydock")
        ).expanduser()
    )
    storage_volume_uuid: str = field(
        default_factory=lambda: os.getenv("DAILYDOCK_STORAGE_VOLUME_UUID", "").strip()
    )
    storage_directory_name: str = field(
        default_factory=lambda: os.getenv(
            "DAILYDOCK_STORAGE_DIRECTORY_NAME", "dailydock"
        )
    )
    storage_check_interval: float = field(
        default_factory=lambda: float(
            os.getenv("DAILYDOCK_STORAGE_CHECK_INTERVAL", "30")
        )
    )
    storage_marker_path: Optional[str] = field(
        default_factory=lambda: os.getenv("DAILYDOCK_STORAGE_MARKER_PATH", "").strip()
        or None
    )

    # Infrastructure monitoring configuration
    monitor_enabled: bool = field(
        default_factory=lambda: os.getenv("DAILYDOCK_MONITOR_ENABLED", "true").lower()
        in ("true", "1", "yes")
    )
    monitor_max_concurrency: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_MONITOR_MAX_CONCURRENCY", "2"))
    )
    monitor_system_interval: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_SYSTEM_INTERVAL", "30"))
    )
    monitor_storage_interval: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_STORAGE_INTERVAL", "60"))
    )
    monitor_connectivity_interval: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_CONNECTIVITY_INTERVAL", "30"))
    )
    monitor_http_interval: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_HTTP_INTERVAL", "60"))
    )
    monitor_service_interval: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_SERVICE_INTERVAL", "30"))
    )
    monitor_check_timeout: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_CHECK_TIMEOUT", "5"))
    )
    monitor_stale_multiplier: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_STALE_MULTIPLIER", "2"))
    )
    monitor_failure_threshold: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_MONITOR_FAILURE_THRESHOLD", "2"))
    )
    monitor_recovery_threshold: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_MONITOR_RECOVERY_THRESHOLD", "1"))
    )
    monitor_cpu_warning_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_CPU_WARNING_PERCENT", "80"))
    )
    monitor_cpu_critical_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_CPU_CRITICAL_PERCENT", "95"))
    )
    monitor_ram_warning_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_RAM_WARNING_PERCENT", "80"))
    )
    monitor_ram_critical_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_RAM_CRITICAL_PERCENT", "90"))
    )
    monitor_storage_warning_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_STORAGE_WARNING_PERCENT", "80"))
    )
    monitor_storage_critical_percent: float = field(
        default_factory=lambda: float(os.getenv("DAILYDOCK_MONITOR_STORAGE_CRITICAL_PERCENT", "90"))
    )
    monitor_retention_days: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_MONITOR_RETENTION_DAYS", "30"))
    )
    monitor_retention_batch_size: int = field(
        default_factory=lambda: int(os.getenv("DAILYDOCK_MONITOR_RETENTION_BATCH_SIZE", "500"))
    )
    monitor_http_endpoints: List[Dict[str, Any]] = field(
        default_factory=lambda: _load_json_list("DAILYDOCK_MONITOR_HTTP_ENDPOINTS")
    )
    monitor_connectivity_probes: List[Dict[str, Any]] = field(
        default_factory=lambda: _load_json_list("DAILYDOCK_MONITOR_CONNECTIVITY_PROBES")
    )
    monitor_storage_targets: List[Dict[str, Any]] = field(
        default_factory=lambda: _load_json_list("DAILYDOCK_MONITOR_STORAGE_TARGETS")
    )

    def __post_init__(self) -> None:
        self.data_dir = _resolve_data_dir(self.data_dir)
        self.storage_mountpoint = Path(self.storage_mountpoint).expanduser()

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

        if not self.storage_mountpoint.is_absolute() or self.storage_mountpoint == Path("/"):
            raise ConfigurationError(
                "storage_mountpoint must be an absolute path other than '/'.",
                module="config",
            )
        if not (0 < self.storage_check_interval <= 86400):
            raise ConfigurationError(
                "storage_check_interval must be between 0 and 86400 seconds.",
                module="config",
            )
        if (
            not self.storage_directory_name
            or self.storage_directory_name in {".", ".."}
            or Path(self.storage_directory_name).name != self.storage_directory_name
        ):
            raise ConfigurationError(
                "storage_directory_name must be a single safe directory name.",
                module="config",
            )
        if self.storage_volume_uuid and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", self.storage_volume_uuid
        ):
            raise ConfigurationError(
                "storage_volume_uuid contains unsupported characters.",
                module="config",
            )
        if self.storage_marker_path is not None:
            marker = Path(self.storage_marker_path)
            if not marker.parts or marker.is_absolute() or any(
                part in {"", ".", ".."} for part in marker.parts
            ):
                raise ConfigurationError(
                    "storage_marker_path must be a safe relative path.",
                    module="config",
                )

        intervals = {
            "monitor_system_interval": self.monitor_system_interval,
            "monitor_storage_interval": self.monitor_storage_interval,
            "monitor_connectivity_interval": self.monitor_connectivity_interval,
            "monitor_http_interval": self.monitor_http_interval,
            "monitor_service_interval": self.monitor_service_interval,
        }
        if not (1 <= self.monitor_max_concurrency <= MAX_MONITOR_CONCURRENCY):
            raise ConfigurationError(
                f"monitor_max_concurrency must be between 1 and {MAX_MONITOR_CONCURRENCY}.",
                module="config",
            )
        for name, value in intervals.items():
            if not (1 <= value <= 86400):
                raise ConfigurationError(
                    f"{name} must be between 1 and 86400 seconds.", module="config"
                )
        if not (0 < self.monitor_check_timeout <= 300):
            raise ConfigurationError(
                "monitor_check_timeout must be greater than 0 and at most 300 seconds.",
                module="config",
            )
        if self.monitor_stale_multiplier <= 0:
            raise ConfigurationError(
                "monitor_stale_multiplier must be greater than 0.", module="config"
            )
        for name, value in {
            "monitor_failure_threshold": self.monitor_failure_threshold,
            "monitor_recovery_threshold": self.monitor_recovery_threshold,
            "monitor_retention_days": self.monitor_retention_days,
            "monitor_retention_batch_size": self.monitor_retention_batch_size,
        }.items():
            if value < 1:
                raise ConfigurationError(f"{name} must be at least 1.", module="config")

        for label, warning, critical in (
            ("CPU", self.monitor_cpu_warning_percent, self.monitor_cpu_critical_percent),
            ("RAM", self.monitor_ram_warning_percent, self.monitor_ram_critical_percent),
            ("storage", self.monitor_storage_warning_percent, self.monitor_storage_critical_percent),
        ):
            if not (0 <= warning <= 100 and 0 <= critical <= 100 and warning < critical):
                raise ConfigurationError(
                    f"{label} warning and critical thresholds must be within 0..100, with warning below critical.",
                    module="config",
                )

        self._validate_monitor_targets(
            self.monitor_http_endpoints, "monitor_http_endpoints", require_url=True
        )
        self._validate_monitor_targets(
            self.monitor_connectivity_probes,
            "monitor_connectivity_probes",
            require_url=True,
        )
        self._validate_monitor_targets(
            self.monitor_storage_targets,
            "monitor_storage_targets",
            require_url=False,
        )

    @staticmethod
    def _validate_monitor_targets(
        targets: List[Dict[str, Any]], setting_name: str, require_url: bool
    ) -> None:
        if len(targets) > 50:
            raise ConfigurationError(
                f"{setting_name} supports at most 50 configured targets.",
                module="config",
            )
        identifiers = set()
        for target in targets:
            target_id = target.get("id")
            if not isinstance(target_id, str) or not target_id.strip():
                raise ConfigurationError(
                    f"Every entry in {setting_name} must have a non-empty id.",
                    module="config",
                )
            if target_id in identifiers:
                raise ConfigurationError(
                    f"Duplicate target id '{target_id}' in {setting_name}.",
                    module="config",
                )
            identifiers.add(target_id)
            if "enabled" in target and not isinstance(target["enabled"], bool):
                raise ConfigurationError(
                    f"Target '{target_id}' enabled must be a boolean.",
                    module="config",
                )
            for name in ("interval", "timeout"):
                if name in target:
                    try:
                        number = float(target[name])
                    except (TypeError, ValueError):
                        number = 0
                    valid_number = (
                        1 <= number <= 86400
                        if name == "interval"
                        else 0 < number <= 300
                    )
                    if not valid_number:
                        raise ConfigurationError(
                            f"Target '{target_id}' {name} is outside the supported range.",
                            module="config",
                        )
            if require_url:
                url = target.get("url")
                parsed = urlparse(url) if isinstance(url, str) else None
                if (
                    parsed is None
                    or parsed.scheme not in {"http", "https"}
                    or not parsed.hostname
                    or parsed.username is not None
                    or parsed.password is not None
                ):
                    raise ConfigurationError(
                        f"Target '{target_id}' in {setting_name} requires an HTTP(S) URL without embedded credentials.",
                        module="config",
                    )
                minimum = target.get("accepted_status_min", 200)
                maximum = target.get("accepted_status_max", 399)
                if (
                    not isinstance(minimum, int)
                    or not isinstance(maximum, int)
                    or not (100 <= minimum <= maximum <= 599)
                ):
                    raise ConfigurationError(
                        f"Target '{target_id}' has an invalid accepted HTTP status range.",
                        module="config",
                    )
            else:
                mount_path = target.get("mount_path")
                if not isinstance(mount_path, str) or not mount_path.startswith("/"):
                    raise ConfigurationError(
                        f"Storage target '{target_id}' must have an absolute mount_path.",
                        module="config",
                    )
                expected_uuid = target.get("expected_uuid")
                if expected_uuid is not None and not isinstance(expected_uuid, str):
                    raise ConfigurationError(
                        f"Storage target '{target_id}' expected_uuid must be a string.",
                        module="config",
                    )
                if expected_uuid == "":
                    raise ConfigurationError(
                        f"Storage target '{target_id}' expected_uuid cannot be empty.",
                        module="config",
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
