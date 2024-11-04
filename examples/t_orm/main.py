from typing import AsyncGenerator
from fastapi import FastAPI
from contextlib import asynccontextmanager
# from config import register_orm



# @asynccontextmanager
# async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
#     async with register_orm(app):
#         yield


# app = FastAPI(title='Test Fastapi', lifespan=lifespan)
