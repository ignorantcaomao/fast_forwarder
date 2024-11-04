from tortoise import fields
from tortoise.models import Model


class User(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=100)
    password = fields.CharField(max_length=255, description="用户名密码")
    email = fields.CharField(max_length=255, description="用户邮箱")

    role = fields.ForeignKeyField('models.Role', related_name='users')

    class Meta:
        table = 'user'


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
