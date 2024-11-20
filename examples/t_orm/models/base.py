from tortoise import fields
from tortoise.models import Model


class TimestampMixin(Model):
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    modified_at = fields.DatetimeField(auto_now=True, description="修改时间")

    class Meta:
        abstract = True


class User(TimestampMixin):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)
    password = fields.CharField(max_length=255, description="用户名密码")
    email = fields.CharField(max_length=255, description="用户邮箱")
    is_active = fields.BooleanField(default=True, description="是否激活")

    role = fields.ForeignKeyField('models.Role', related_name='users')

    class Meta:
        table = 'user'
        table_description = '用户表'


class Role(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)

    permission = fields.ManyToManyField('models.Permission', related_name='roles', through='role_permission')
    role: fields.ForeignKeyRelation["models.User"]

    class Meta:
        table = 'role'


class Permission(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)

    role: fields.ManyToManyRelation["models.Role"]

    class Meta:
        table = 'permission'
