"""
Centralized application configuration using Pydantic Settings.

Loads values from environment variables and .env file with type safety
and validation. No more hardcoded strings scattered across modules.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Type-safe application configuration loaded from environment variables."""

    # Database
    database_url: str = Field(
        default="postgresql://postgres:password@localhost:5432/orders_db",
        description="PostgreSQL connection string",
    )
    replication_slot_name: str = Field(
        default="orders_realtime_slot",
        description="Logical replication slot name for CDC",
    )

    # Server
    host: str = Field(default="0.0.0.0", description="Server bind address")
    port: int = Field(default=8000, description="Server port")

    # Logging
    log_level: str = Field(default="INFO", description="Logging verbosity")

    # CORS
    cors_origins: str = Field(
        default="*",
        description="Comma-separated list of allowed CORS origins",
    )

    @property
    def cors_origin_list(self) -> list[str]:
        """Parse comma-separated CORS origins into a list."""
        return [origin.strip() for origin in self.cors_origins.split(",")]

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


# Singleton instance — import this across the application
settings = Settings()
