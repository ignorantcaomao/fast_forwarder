"""用户视图"""
from typing import Any

from fastapi import APIRouter

from app.core.Response import fail, success
from app.core.Utils import get_password_hash
from app.models import User
from app.schemas.user import UserCreate

router = APIRouter(prefix="/user")


@router.post("", summary="添加用户")
async def create_user(user: UserCreate) -> dict[str, Any]:
    """创建新用户

    Args:
        user (UserCreate): _description_

    Returns:
        dict[str, Any]: _description_
    """

    try:
        await User.get_or_none(username=user.username)

    except Exception as e:
        print(e)
        return fail(msg=f"{user.username} 已存在")

    hashed_password = get_password_hash(user.password)
    user.password = hashed_password

    user_obj: User = await User.create(**user.dict())
    if not user_obj:
        return fail(msg=f"创建用户{user.username}失败")
    return success(msg=f"{user_obj.username} 创建成功")
