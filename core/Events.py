from fastapi import FastAPI
from typing import Callable


def startup(app: FastAPI) -> Callable:
    """
    Fastapi 启动完成事件
    ：param app: Fastapi
    : return start_app
    """

    async def start_app() -> None:
        print("fastapi 已启动")
        # app.state.cache = await

    return start_app


def stopping(app: FastAPI) -> Callable:
    """
    Fastapi 停止事件
    ：param app: Fastapi
    : return stop_app
    """

    async def stop_app() -> None:
        print("fastapi 已停止")

    return stop_app
