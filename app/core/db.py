from functools import partial
from tortoise.contrib.fastapi import RegisterTortoise
from app.core.config import settings


DATABASE_URLS: dict[str, str] = {
    "sqlite": "sqlite://db.sqlite3",
    "mysql": f"mysql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_DATABASE}",
    "postgres": f"postgres://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_DATABASE}",
}


# 获取数据库连接信息
def get_database_url() -> str:
    # 从环境变量中获取数据库类型（默认为 SQLite）
    db_type = settings.DB_TYPE
    # 根据数据库类型返回相应的数据库 URL
    return DATABASE_URLS.get(db_type, DATABASE_URLS["sqlite"])


DB_ORM_CONFIG = {
    "connections": {
        "default": get_database_url(),
    },
    "apps": {
        "base": {
            "models": ["app.models.base"],
            "default_connection": "default",
        }
    },
    "use_tz": False,
    "timezone": "Asia/Shanghai",
}

print(DB_ORM_CONFIG)

register_tortoise = partial(
    RegisterTortoise,
    config=DB_ORM_CONFIG,
    generate_schemas=True,
    add_exception_handlers=True,
)
