from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import httpx
import typer

from .config import CLIConfig, ensure_token, load_config


class APIError(Exception):
    def __init__(self, status_code: int, detail: Any) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API error {status_code}: {detail}")


class APIClient:
    def __init__(self, config: CLIConfig | None = None) -> None:
        self.config = config or load_config()
        env_base_url = os.getenv("NL2SQL_API_URL")
        if env_base_url:
            self.config.base_url = env_base_url
        self._client = httpx.Client(base_url=self.config.base_url, timeout=30.0)

    def _headers(self, require_auth: bool = True) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if require_auth:
            token = ensure_token(self.config)
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def request(self, method: str, url: str, json: Any | None = None, require_auth: bool = True) -> Any:
        response = self._client.request(method, url, json=json, headers=self._headers(require_auth))
        if response.status_code >= 400:
            detail = response.json().get("detail") if response.headers.get("content-type", "").startswith("application/json") else response.text
            raise APIError(response.status_code, detail)
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        return response.text

    @contextmanager
    def stream(self, method: str, url: str, require_auth: bool = True, **kwargs: Any) -> Iterator[httpx.Response]:
        with self._client.stream(method, url, headers=self._headers(require_auth), **kwargs) as response:
            if response.status_code >= 400:
                detail = response.text
                raise APIError(response.status_code, detail)
            yield response

    def close(self) -> None:
        self._client.close()


@contextmanager
def api_client(config: CLIConfig | None = None) -> Iterator[APIClient]:
    client = APIClient(config=config)
    try:
        yield client
    finally:
        client.close()
