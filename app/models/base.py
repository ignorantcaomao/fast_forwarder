"""基础表"""
from tortoise import fields, models


class TimestampMixin(models.Model):
    """基本表"""
    created_time = fields.DatetimeField(auto_now_add=True, description="创建时间")
    modified_time = fields.DatetimeField(auto_now=True, description="更新时间")

    class Meta:
        abstract = True
        ordering = ["-created"]
