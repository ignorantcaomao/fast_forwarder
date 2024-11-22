from app.schemas.user import UserCreate
from app.models.base import User
from fastapi import APIRouter
from app.core.Response import fail, success
from typing import Any

router = APIRouter(prefix="/user")


@router.post("", summary="添加用户")
async def create_user(user: UserCreate) -> dict[str, Any]:
    try:
        await User.get_or_none(username=user.username)

    except Exception as e:
        return fail(msg=f"{user.username} 已存在")

    user_obj: User = await User.create(**user.dict())
    if not user_obj:
        return fail(msg=f"创建用户{user.username}失败")
    return success(msg=f"{user_obj.username} 创建成功")
