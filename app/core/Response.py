"""设置统一响应返回"""
from typing import Any


def base_response(code, msg, data=None) -> dict[str, Any]:
    """_summary_

    Args:
        code (_type_): _description_
        msg (_type_): _description_
        data (_type_, optional): _description_. Defaults to None.

    Returns:
        dict[str, Any]: _description_
    """
    if data is None:
        data = []
    return {"code": code, "msg": msg, "data": data}


def success(data=None, msg="") -> dict[str, Any]:
    """成功返回的数据体

    Args:
        data (_type_, optional): _description_. Defaults to None.
        msg (str, optional): _description_. Defaults to "".

    Returns:
        dict[str, Any]: _description_
    """
    return base_response(200, msg, data)


def fail(code=-1, msg="", data=None) -> dict[str, Any]:
    """失败返回的数据体

    Args:
        code (int, optional): _description_. Defaults to -1.
        msg (str, optional): _description_. Defaults to "".
        data (_type_, optional): _description_. Defaults to None.

    Returns:
        dict[str, Any]: _description_
    """
    return base_response(code, msg, data)
