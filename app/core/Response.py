from typing import List, Any


def base_response(code, msg, data=None) -> dict[str, Any]:
    if data is None:
        data = []
    return {"code": code, "msg": msg, "data": data}


def success(data=None, msg="") -> dict[str, Any]:
    return base_response(200, msg, data)


def fail(code=-1, msg="", data=None) -> dict[str, Any]:
    return base_response(code, msg, data)
