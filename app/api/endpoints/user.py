"""用户视图"""

from typing import Any, List

import jwt
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from jose import JWTError
from pydantic import EmailStr

from app.core.mail import send_email
from app.core.Response import fail, success
from app.core.Utils import create_confirmation_token, get_password_hash
from app.models import User
from app.schemas.user import UserCreate
from app.core.config import settings

router = APIRouter(prefix="/user")


# @router.post("", summary="添加用户")
# async def create_user(user: UserCreate) -> dict[str, Any]:
#     """创建新用户

#     Args:
#         user (UserCreate): _description_

#     Returns:
#         dict[str, Any]: _description_
#     """

#     try:
#         await User.get_or_none(username=user.username)

#     except Exception as e:
#         print(e)
#         return fail(msg=f"{user.username} 已存在")

#     hashed_password = get_password_hash(user.password)
#     user.password = hashed_password

#     user_obj: User = await User.create(**user.dict())
#     if not user_obj:
#         return fail(msg=f"创建用户{user.username}失败")
#     return success(msg=f"{user_obj.username} 创建成功")


@router.post("/register/", summary="用户注册")
async def register(
    request: Request, user: UserCreate, background_tasks: BackgroundTasks
):
    print(f"email: {user.email}")
    """用户注册函数"""
    # 判断用户的邮箱是否注册过
    user_obj: User | None = await User.get_or_none(email=user.email)
    if user_obj:
        return fail(msg="邮箱已注册")

    # 密码加密
    hashed_password = get_password_hash(user.password)
    # 创建确认令牌
    token = create_confirmation_token(user.email)
    confirm_url = f"http://127.0.0.1:8000/api/v1/admin/user/confirm/?token={token}"

    # 发送邮件
    try:
        await send_email(
            subject="Confirm Your Registration",
            email_to=user.email,
            context={
                "username": user.username,
                "confirm_url": confirm_url,
                "subject": "Confirm Your Registration",
            },
            background_tasks=background_tasks,
            template_name="email_template.html",
        )
        return success(msg="注册成功，请检查您的邮箱并点击激活链接")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")


@router.get("/confirm/", summary="用户确认")
async def confirm(token: str):
    try:
        payload: Any = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        email: EmailStr = payload["sub"]

        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")
        return success(msg="激活成功")

    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid or expired token")


@router.get("/list/", summary="用户列表")
async def get_users():
    user_list: List[User] = await User.all()

    # for user in user_list:
    #     print(user.username)

    return user_list
