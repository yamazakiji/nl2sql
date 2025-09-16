from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = "sqlite+aiosqlite:///./data/metadata.db"
    redis_url: str = "redis://127.0.0.1:6379"
    object_store_path: Path = Path("./data/artifacts")
    cors_origins: list[str] = ["http://localhost:5173"]
    dev_token: str = "dev-token"
    sse_log_retention: int = 500
    default_query_limit: int = 1000
    environment: str = "dev"
    inference_provider: Literal["stub", "openai"] = "openai"
    openai_api_key: str | None = "123123"
    openai_model: str = "Qwen/Qwen3-8B"
    openai_api_base: str | None = "http://localhost:8000/v1"
    openai_temperature: float = 0.0

    model_config = SettingsConfigDict(
        env_prefix="NL2SQL_",
        env_file=".env",
        extra="allow",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.object_store_path.mkdir(parents=True, exist_ok=True)

    url = make_url(settings.database_url)
    if url.drivername.startswith("sqlite") and url.database:
        Path(url.database).parent.mkdir(parents=True, exist_ok=True)
    return settings
