from tortoise import fields, models


class TimestampMixin(models.Model):
    created_time = fields.DatetimeField(auto_now_add=True, description="创建时间")
    modified_time = fields.DatetimeField(auto_now=True, description="更新时间")

    class Meta:
        abstract = True


class Role(TimestampMixin):
    role_name = fields.CharField(max_length=255, unique=True, description="角色名称")
    role_status = fields.BooleanField(
        default=False, description="True： 启用 False: 禁用"
    )
    role_desc = fields.CharField(null=True, max_length=255, desription="角色描述")
    user: fields.ManyToManyRelation["User"] = fields.ManyToManyField(
        "base.User", related_name="role", on_delete=fields.CASCADE
    )

    def __str__(self) -> str:
        return self.role_name

    class Meta:
        table: str = "role"
        table_description: str = "角色表"


class Access(TimestampMixin):
    access_name = fields.CharField(max_length=255, unique=True, description="权限名称")
    parent_id = fields.IntField(default=0, description="父ID")
    scopes = fields.CharField(unique=True, max_length=255, description="权限范围标识")
    access_desc = fields.CharField(max_length=255, description="权限描述")

    def __str__(self) -> str:
        return self.access_name

    class Meta:
        table: str = "access"
        table_description: str = "权限表"


class User(TimestampMixin):
    username = fields.CharField(max_length=128, unqiue=True, description="用户名")
    password = fields.CharField(max_length=255, description="密码")
    email = fields.CharField(null=True, max_length=255, description="邮箱")
    is_active = fields.BooleanField(default=False, description="用户状态")
    is_superuser = fields.BooleanField(default=False, description="是否是超级用户")
    phone = fields.CharField(max_length=11, description="手机号码")

    role: fields.ManyToManyRelation[Role]

    def __str__(self) -> str:
        return self.username

    class Meta:
        table: str = "user"
        table_description: str = "用户表"
