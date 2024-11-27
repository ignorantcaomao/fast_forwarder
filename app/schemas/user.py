"""user序列化"""
from pydantic import BaseModel, EmailStr, Field
from pydantic_extra_types.phone_numbers import PhoneNumber


class UserCreate(BaseModel):
    """新建用户使用的模型

    Args:
        BaseModel (_type_): _description_
    """
    username: str = Field(min_length=3, max_length=20)
    password: str = Field(min_length=8, max_length=20)
    phone: PhoneNumber
    email: EmailStr
