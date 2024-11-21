from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
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


settings = Settings()
