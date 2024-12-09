"""工具类"""

from datetime import datetime, timedelta

from fastapi import HTTPException, Request
from jose import JWTError, jwt
from passlib.context import CryptContext

from urllib.parse import urljoin

from app.core.config import settings

SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password: str) -> str:
    """密码加密

    Args:
        password (_type_): _description_

    Returns:
        _type_: _description_
    """
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""

    return pwd_context.verify(plain_password, hashed_password)


def create_confirmation_token(email: str) -> str:
    """生成令牌函数"""
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": email, "exp": expire}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_confirmation_token(token: str) -> str:
    """验证令牌函数"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid token or expired token")


# 动态生成确认链接的依赖
def get_confirmation_url(request: Request, router_prefix: str) -> str:
    # 获取基础 URL (包含协议和域名)
    base_url: str = str(request.base_url)
    # 拼接完整的确认链接
    confirmation_url: str = urljoin(base_url, f"{router_prefix}/verify-email")
    return confirmation_url
