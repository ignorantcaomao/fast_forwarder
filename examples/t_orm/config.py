import os
from tortoise.contrib.fastapi import RegisterTortoise
from functools import partial

# register_orm = partial(
#     RegisterTortoise,
#     # db_url=os.getenv("DB_URL", "sqlite://db.sqlite3"),
#     db_url="sqlite://db.sqlite3",
#     modules={"models": ["models", "aerich.models"]},
#     generate_schemas=True,
#     add_exception_handlers=True,
# )

# register_orm = RegisterTortoise(
#     db_url="sqlite://db.sqlite3",
#     modules={"models": ["models", "aerich.models"]},
#     generate_schemas=True,
#     add_exception_handlers=True,
# )

#
# TORTOISE_ORM = {
#     "connections": {
#         "default": "sqlite://db.sqlite3"
#     },
#     "apps": {
#         "models": {
#             "models": ["models", "aerich.models"],  # 添加你的模型模块和 aerich 迁移模型
#             "default_connection": "default",
#         },
#     },
#     "use_tz": False,  # 是否使用时区
#     "timezone": "UTC",  # 时区设置
# }

import ssl

ctx = ssl.create_default_context()
# And in this example we disable validation...
# Please don't do this. Look at the official Python ``ssl`` module documentation
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Here we do a verbose init

TORTOISE_ORM = {
    "connections": {
        "default": {
            "engine": "tortoise.backends.mysql",
            "credentials": {
                "database": None,
                "host": "djswork.asia",
                "password": "Mysql@djs",
                "port": 3306,
                "user": "root",
                # "ssl": ctx  # Here we pass in the SSL context
            }
        }
    },
    "apps": {
        "models": {
            "models": ["models", "aerich.models"],
            "default_connection": "default",
        }
    },
}
