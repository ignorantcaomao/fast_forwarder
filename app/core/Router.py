"""应用总路由"""
from fastapi import APIRouter

from app.api.api import apirouter

router = APIRouter()

router.include_router(apirouter)
