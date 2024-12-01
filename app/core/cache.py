"""redis 配置文件"""
import aioredis

from app.core.config import settings


async def init_redis():
    """init_redis"""
    REDIS = await aioredis.from_url(
        f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}",
        password=settings.REDIS_PASSWORD,
        db=settings.REDIS_DB,
        encoding="utf-8",
        decode_responses=True,
    )
    return REDIS
