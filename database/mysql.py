from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise

# -----------------------数据库配置-----------------------------------
DB_ORM_CONFIG = {

}

{
    'connections': {
        # Dict format for connection
        'default': {
            'engine': 'tortoise.backends.asyncpg',
            'credentials': {
                'host': 'localhost',
                'port': '5432',
                'user': 'tortoise',
                'password': 'qwerty123',
                'database': 'test',
            }
        },
        # Using a DB_URL string
        'default': 'postgres://postgres:qwerty123@localhost:5432/events'
    },
    'apps': {
        'models_old': {
            'models_old': ['__main__'],
            # If no default_connection specified, defaults to 'default'
            'default_connection': 'default',
        }
    }
}


async def register_mysql(app: FastAPI):
    register_tortoise(
        app,
        config=DB_ORM_CONFIG,
        generate_schemas=False,
        add_exception_handlers=True
    )
