from pydantic import Field, BaseModel, ValidationError, constr
from typing import Optional
from re import fullmatch


def phone_number(value: str) -> str:
    if not fullmatch(r"/^(?:(?:\+|00)86)?1\d{10}$/", value):
        raise ValueError("不是有效的中国大陆手机号")
    return value


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=20)
    password: str = Field(min_length=8, max_length=20)
    phone: constr(min_length=11, max_length=11) = phone_number  # type: ignore
    # email: Optional[str] = Field(max_length=50)
