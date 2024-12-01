"""用户视图"""
from typing import Any

import jwt
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi_mail import FastMail, MessageSchema, MessageType
from jose import JWTError

from app.core.email import MAIL_CONF, render_email_template
from app.core.Response import fail, success
from app.core.Utils import create_confirmation_token, get_password_hash
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


@router.post("/register/", summary="用户注册")
async def register(request: Request, user: UserCreate, background_tasks: BackgroundTasks):
    # 判断用户的邮箱是否注册过
    user_obj = await User.get_or_none(email=user.email)
    if user_obj:
        return fail(msg="邮箱已注册")

    # 密码加密
    hashed_password = get_password_hash(user.password)
    # 创建确认令牌
    token = create_confirmation_token(user.email)
    confirm_url = f"http://127.0.0.1:8000/confirm/?token={token}"

    # 渲染邮件模板
    html_context = render_email_template(
        "email_template.html",
        {
            "confirm_url": confirm_url,
            "email": user.email
        }

    )

    # 配置邮件内容
    message = MessageSchema(
        subject="Confirm your registration",
        recipients=[user.email],
        body=html_context,
        subtype=MessageType.html
    )

    # 发送邮件
    fm = FastMail(MAIL_CONF)

    background_tasks.add_task(fm.send_message, message)

    return success(msg="注册成功，请检查您的邮箱并点击激活链接")


async def confirm(token: str):
    try:
        payload = jwt.decode(token, ECRET_KEY, algorithms=[ALGORITHM])
        email = payload["sub"]
        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")

    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
