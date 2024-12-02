"""用户表"""

from tortoise import fields

from .base import TimestampMixin


class User(TimestampMixin):
    """用户模型"""

    username = fields.CharField(max_length=128, unqiue=True, description="用户名")
    password = fields.CharField(max_length=255, description="密码")
    email = fields.CharField(null=True, max_length=255, description="邮箱")
    is_active = fields.BooleanField(default=False, description="用户状态")
    is_superuser = fields.BooleanField(default=False, description="是否是超级用户")
    phone = fields.CharField(max_length=30, description="手机号码")

    role: fields.ForeignKeyRelation["models.Role"]

    def __str__(self) -> str:
        return self.username

    class Meta:
        table: str = "user"
        table_description: str = "用户表"
