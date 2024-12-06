"""redis 配置文件"""

# import aioredis
# from aioredis import Redis

from redis.asyncio import client

from app.core.config import settings


# async def init_redis():
#     """init_redis"""
#     if settings.REDIS_PASSWORD:
#         REDIS: Redis = await aioredis.from_url(
#             f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}",
#             password=settings.REDIS_PASSWORD,
#             db=settings.REDIS_DB,
#             encoding="utf-8",
#             decode_responses=True,
#         )
#     else:
#         REDIS: Redis = await aioredis.from_url(
#             f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}",
#             db=settings.REDIS_DB,
#             encoding="utf-8",
#             decode_responses=True,
#         )

#     return REDIS


async def init_redis():
    """init_redis"""
    if settings.REDIS_PASSWORD:
        redis_client = client.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
        )
    else:
        redis_client = client.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True,
        )

    return redis_client
