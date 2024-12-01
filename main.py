from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi_lifespan_manager import LifespanManager, State

from app.core import Router
from app.core.cache import init_redis
from app.core.config import settings
from app.core.db import register_tortoise

manager = LifespanManager()


async def setup_db(app: FastAPI) -> AsyncIterator[State]:
    """Setup database connection"""
    db = await register_tortoise(app)
    yield {"db": db}
    await db.close_orm()


async def setup_cache(app: FastAPI) -> AsyncIterator[State]:
    """Setup cache connection"""
    # Implement cache setup logic here
    cache = await init_redis()
    yield {"cache": cache}
    await cache.close_redis()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.PROJECT_VERSION,
    lifespan=manager,
)

app.include_router(Router.router)
