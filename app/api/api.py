from fastapi import APIRouter
from app.api.endpoints import user

apirouter = APIRouter(prefix="/api/v1")

apirouter.include_router(user.router, prefix="/admin", tags=["用户管理"])
