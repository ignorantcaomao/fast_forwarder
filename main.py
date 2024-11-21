from fastapi import FastAPI
from app.core.config import settings
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from app.core.db import register_tortoise


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with register_tortoise(app):
        yield


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.PROJECT_VERSION,
    lifespan=lifespan,
)
