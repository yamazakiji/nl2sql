from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from nl2sql.api.routers import connectors, deployments, health, inference, projects, runs, train, metrics as metrics_router
from nl2sql.config import get_settings
from nl2sql.db import init_db, shutdown_db
from nl2sql.metrics.store import metrics_store
from nl2sql.observability import configure_logging


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging()

    app = FastAPI(title="nl2sql", version="0.1.0", docs_url="/docs", redoc_url="/redoc")

    app.include_router(health.router)
    app.include_router(projects.router)
    app.include_router(connectors.router)
    app.include_router(train.router)
    app.include_router(deployments.router)
    app.include_router(inference.router)
    app.include_router(runs.router)
    app.include_router(metrics_router.router)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):  # type: ignore[override]
        with metrics_store.track():
            response = await call_next(request)
        return response

    @app.on_event("startup")
    async def on_startup() -> None:
        await init_db()

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        await shutdown_db()

    return app


app = create_app()
