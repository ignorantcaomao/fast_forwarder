from contextlib import asynccontextmanager
from typing import AsyncGenerator, AsyncIterator

from fastapi import FastAPI
from fastapi_lifespan_manager import LifespanManager, State

from app.core import Router
from app.core.config import settings
from app.core.db import register_tortoise

manager = LifespanManager()


async def setup_db(app: FastAPI) -> AsyncIterator[State]:
    db = await register_tortoise(app)
    yield {"db": db}
    await db.close_orm()

# @asynccontextmanager
# async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
#     async with register_tortoise(app):
#         yield


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.PROJECT_VERSION,
    lifespan=manager,
)

app.include_router(Router.router)
