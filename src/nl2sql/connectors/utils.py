from __future__ import annotations

from sqlalchemy.engine import make_url


def ensure_sqlite_read_only(dsn: str) -> str:
    url = make_url(dsn)
    if not url.drivername.startswith("sqlite"):
        return dsn

    query = dict(url.query)
    query.setdefault("mode", "ro")
    return url.set(query=query).render_as_string(hide_password=False)


def mask_dsn(dsn: str) -> str:
    url = make_url(dsn)
    if url.password:
        url = url.set(password="***")
    return url.render_as_string(hide_password=False)
