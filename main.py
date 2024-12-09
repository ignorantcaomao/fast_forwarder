from typing import AsyncIterator

from fastapi import FastAPI
from fastapi_lifespan_manager import LifespanManager, State

from app.core import Router

from app.core.cache import init_redis
from app.core.config import settings
from app.core.db import init_db, close_db
import logging

manager = LifespanManager()

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# 打印tortoise 执行过程中的sql 语句
logging.getLogger("tortoise.db_client").setLevel(logging.DEBUG)


@manager.add
async def setup_db(app: FastAPI) -> AsyncIterator[State]:
    """Setup database connection"""
    # db = await register_tortoise(app)
    # yield {"db": db}
    db = await init_db()
    yield {"db": db}
    await close_db()


@manager.add
async def setup_cache(app: FastAPI) -> AsyncIterator[State]:
    """Setup cache connection"""
    print("设置Redis 连接信息")
    # Implement cache setup logic here
    cache = await init_redis()
    yield {"cache": cache}
    await cache.aclose()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.PROJECT_DESCRIPTION,
    version=settings.PROJECT_VERSION,
    lifespan=manager,
)

app.include_router(Router.router)
