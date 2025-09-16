from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer

CONFIG_DIR = Path.home() / ".config" / "nl2sql"
CONFIG_PATH = CONFIG_DIR / "config.json"


@dataclass
class CLIConfig:
    base_url: str = "http://localhost:8001"
    token: str | None = None
    last_run_id: str | None = None


def load_config() -> CLIConfig:
    if not CONFIG_PATH.exists():
        return CLIConfig()
    data = json.loads(CONFIG_PATH.read_text())
    return CLIConfig(
        base_url=data.get("base_url", "http://localhost:8001"),
        token=data.get("token"),
        last_run_id=data.get("last_run_id"),
    )


def save_config(config: CLIConfig) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(config.__dict__, indent=2))


def ensure_token(config: CLIConfig) -> str:
    if config.token:
        return config.token
    raise typer.BadParameter("Run 'nl2sql login' to configure an API token.")
