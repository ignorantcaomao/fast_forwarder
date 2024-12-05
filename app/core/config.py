"""应用配置文件"""

from typing import List

from pydantic import EmailStr, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """应用的整体配置

    Args:
        BaseSettings (_type_): _description_
    """

    model_config = SettingsConfigDict(
        # Use top level .env file (one level above ./backend/)
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    PROJECT_VERSION: str = "0.0.1"
    PROJECT_NAME: str = "djs"
    PROJECT_DESCRIPTION: str = ""

    DB_TYPE: str = "mysql"  # default=sqlite , others: postgres, sqlite
    DB_HOST: str = ""
    DB_PORT: int = 3306
    DB_USER: str = ""
    DB_PASSWORD: str = ""
    DB_DATABASE: str = ""

    # 安全信息
    SECRET_KEY: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1

    # Redis 配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = None
    REDIS_DB: int = 0
    REDIS_MODE: str = "single"

    # Kafka 配置
    KAFKA_BROKERS: List[str] = Field(default=["localhost:9092"])
    KAFKA_TOPIC: str = "example_topic"
    KAFKA_GROUP_ID: str = "example_group"

    # RabbitMQ 配置
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"

    MAIL_USERNAME: EmailStr = "caomaodjs@163.com"
    MAIL_PASSWORD: SecretStr = SecretStr("BWRyYpGeb3mw3Efq")
    MAIL_FROM: EmailStr = "caomaodjs@163.com"
    MAIL_PORT: int = 465
    MAIL_SERVER: str = "smtp.163.com"
    MAIL_FROM_NAME: str = "caomaodjs"

    # 模板文件路径
    TEMPLATES_DIR: str = "templates"


settings = Settings()


# if __name__ == "__main__":
#     print(settings.SECRET_KEY)
#     print(settings.ACCESS_TOKEN_EXPIRE_MINUTES)
#     print(settings.REDIS_HOST)
#     print(settings.REDIS_PORT)
#     print(settings.REDIS_PASSWORD)
#     print(settings.REDIS_DB)
#     print(settings.REDIS_MODE)
#     print(settings.KAFKA_BROKERS)
#     print(settings.KAFKA_TOPIC)
#     print(settings.KAFKA_GROUP_ID)
#     print(settings.RABBITMQ_HOST)
#     print(settings.RABBITMQ_PORT)
#     print(settings.RABBITMQ_USER)
#     print(settings.RABBITMQ_PASSWORD)
#     print(settings.MAIL_USERNAME)
#     print(settings.MAIL_PASSWORD)
#     print(settings.MAIL_FROM)
#     print(settings.MAIL_PORT)
#     print(settings.MAIL_SERVER)
#     print(settings.TEMPLATES_DIR)
