"""Application configuration settings."""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Settings:
    """DailyDock backend configuration loaded from environment variables."""

    app_name: str = "DailyDock"
    app_version: str = "0.1.0"
    env: str = field(
        default_factory=lambda: os.getenv("DAILYDOCK_ENV", "development")
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


settings = Settings()

