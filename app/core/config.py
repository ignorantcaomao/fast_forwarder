"""应用配置文件"""
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
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30


settings = Settings()
