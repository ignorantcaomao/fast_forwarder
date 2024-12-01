"""email 配置文件"""
from fastapi import BackgroundTasks
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema, MessageType
from jinja2 import Environment, FileSystemLoader

from app.core.config import settings

# 配置模板路径
templates = Environment(loader=FileSystemLoader(settings.TEMPLATES_DIR))

# email 配置文件
MAIL_CONF = ConnectionConfig(
    MAIL_USERNAME=settings.MAIL_USERNAME,      # 替换为163邮箱
    MAIL_PASSWORD=settings.MAIL_PASSWORD,     # 替换为授权码
    MAIL_FROM=settings.MAIL_FROM,
    MAIL_PORT=settings.MAIL_PORT,
    MAIL_SERVER=settings.MAIL_SERVER,
    MAIL_FROM_NAME=settings.MAIL_FROM_NAME,
    MAIL_TLS=False,
    MAIL_SSL=True,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True,
)


def render_email_template(template_name: str, context: dict) -> str:
    """
    使用 Jinja2 渲染 HTML 邮件模板
    :param template_name: 模板文件名
    :param context: 渲染模板所需的上下文数据
    :return: 渲染后的 HTML 字符串
    """
    template = templates.get_template(template_name)
    return template.render(context)


async def send_email(
    subject: str,
    email_to: str,
    context: dict,
    background_tasks: BackgroundTasks,
    template_name: str = "email_template.html",
):
    """
    发送邮件工具函数
    :param subject: 邮件主题
    :param email_to: 收件人
    :param template_name: Jinja2 模板文件名
    :param context: 渲染模板所需的上下文数据
    :param background_tasks: FastAPI 的后台任务实例
    """
    html_content = render_email_template(template_name, context)

    message = MessageSchema(
        subject=subject,
        recipients=[email_to],
        body=html_content,
        subtype=MessageType.html,  # 使用 HTML 格式
    )

    fm = FastMail(MAIL_CONF)
    background_tasks.add_task(fm.send_message, message)
