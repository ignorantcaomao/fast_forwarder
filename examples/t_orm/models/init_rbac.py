from models import Role, Permission
from tortoise.transactions import in_transaction


async def init_role_and_permissions():
    async with in_transaction() as conn:
        # 创建权限
        read = await Permission.create(name='read', using_db=conn)
        create = await Permission.create(name='create', using_db=conn)
        delete = await Permission.create(name='delete', using_db=conn)

        # 创建角色并绑定权限
        admin_role = await Role.create(name='admin', using_db=conn)
        editor_role = await Role.create(name='editor', using_db=conn)
        viewer_role = await Role.create(name='viewer', using_db=conn)

        await admin_role.permission.add(read, create, delete, using_db=conn)
        await editor_role.permission.add(read, create, using_db=conn)
        await viewer_role.permission.add(read, using_db=conn)
