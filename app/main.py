import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.config import Settings, load_settings
from app.db import create_engine, create_session_factory

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = settings if settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(settings)
        app.state.session_factory = create_session_factory(engine)
        logger.info("Payment API started")
        try:
            yield
        finally:
            await engine.dispose()
            logger.info("Payment API stopped")

    app = FastAPI(
        title="Payment API",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.include_router(router)
    return app
