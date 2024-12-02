"""角色表"""

from tortoise import fields

from .base import TimestampMixin


class Role(TimestampMixin):
    """#-
    Role: 定义资源及其操作权限#-
    """  # -

    role_name = fields.CharField(max_length=255, unique=True, description="角色名称")
    role_status = fields.BooleanField(
        default=False, description="True： 启用 False: 禁用"
    )
    role_desc = fields.CharField(null=True, max_length=255, desription="角色描述")
    # 存储资源及其操作 {"users": ["create", "read"], "posts": ["read"]}#-
    permissions = fields.JSONField()

    def __str__(self) -> str:
        return self.role_name

    class Meta:
        table: str = "role"
        table_description: str = "角色表"


class RoleBinding(TimestampMixin):
    """#-
    RoleBinding: 将 Role 绑定到用户#-
    """  # -

    user_id = fields.CharField(max_length=50)  # 用户唯一标识#-
    role = fields.ForeignKeyField("models.Role", related_name="bindings")

    class Meta:
        table: str = "role_binding"
        table_description: str = "用户角色绑定表"
