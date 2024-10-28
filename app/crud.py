from sqlmodel import Session

from app.core.security import get_password_hash
from app.models import User, UserCreate

# *的作用是限制该函数只能以关键字参数调用法
'''
# 方式一 位置参数调用法  
create_user(session, user_create)
# 方式二 关键字参数调用法
create_user(session=session, user_create=user_create)
'''
def create_user(*, session: Session, user_create: UserCreate) -> User:
    db_obj = User.model_validate(
        user_create, update={"hashed_password": get_password_hash(user_create.password)}
    )
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj
