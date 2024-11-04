# from fastapi import FastAPI
# from t_orm.core.config import settings
#
#
# t_orm = FastAPI(
#     title=settings.PROJECT_NAME,
#     openapi_url=f"{settings.API_V1_STR}/openapi.json",
#     docs_url=f"{settings.API_V1_STR}/docs"
# )
#
#
# @t_orm.get("/")
# async def root():
#     return {"message": settings}
#
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from typing import List
import asyncio

app = FastAPI()

# 用于保存所有的客户端连接
clients: List[asyncio.Queue] = []


# SSE生成器，用于向客户端发送事件流
async def event_generator():
    queue = asyncio.Queue()  # 每个客户端拥有自己的消息队列
    clients.append(queue)
    try:
        while True:
            data = await queue.get()  # 等待新消息
            yield f"data: {data}\n\n"
    except asyncio.CancelledError:
        clients.remove(queue)  # 客户端断开连接时移除其队列
        raise


# SSE endpoint，用于客户端连接
@app.get("/sse")
async def sse(request: Request):
    # 返回SSE流
    return StreamingResponse(event_generator(), media_type="text/event-stream")

# @t_orm.post("/sse")
# async def sse(client_id: str, request: Request):
#     # 返回SSE流
#     return StreamingResponse(event_generator(), media_type="text/event-stream")


# 广播消息到所有客户端
async def broadcast_message(message: str):
    for queue in clients:
        await queue.put(message)  # 将消息放入所有客户端的队列中


# 定期发送广播消息
@app.on_event("startup")
async def startup_event():
    async def send_messages():
        count = 0
        while True:
            await asyncio.sleep(5)  # 每5秒发送一次消息
            await broadcast_message(f"Server message: {count}")
            count += 1

    asyncio.create_task(send_messages())
