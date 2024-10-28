from dotenv import load_dotenv, find_dotenv
from pydantic import BaseSettings

class Config(BaseSettings):
    # 加载环境变量
    load_dotenv(find_dotenv(), override=True)
    # 调试模式
    APP_DEBUG: bool = True
    VERSION: str = "0.1.0"
    PROJECT_NAME: str = "FastForwarder"
    DESCRIPTION: str = "FastForwarder"

settings = Config()