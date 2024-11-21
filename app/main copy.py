from fastapi import FastAPI, HTTPException
from app.models.test import ItemPayload
import redis

app = FastAPI()

# grocery_list: dict[int, ItemPayload] = {}
redis_client = redis.StrictRedis(
    host="djswork.asia", port=6379, db=0, decode_responses=True
)


@app.get("/")
def root():
    return {"message": "hello"}


@app.post("/items/{item_name}/{quantity}")
def add_item(item_name: str, quantity: int) -> dict[str, ItemPayload]:
    if quantity <= 0:
        raise HTTPException(
            status_code=400, detail="Quantity must be a positive integer"
        )
    # check if item already exists
    item_id_str: str | None = redis_client.hget("item_name_to_id", item_name)

    if item_id_str is not None:
        item_id = int(item_id_str)
        redis_client.hincrby(f"item_id:{item_id}", "quantity", quantity)
    else:
        # Generate an ID for the item
        item_id: int = redis_client.incr("item_ids")
        redis_client.hset(
            f"item_id:{item_id}",
            mapping={
                "item_id": item_id,
                "item_name": item_name,
                "quantity": quantity,
            },
        )
        redis_client.hset("item_name_to_id", item_name, item_id)

    return {
        "item": ItemPayload(
            item_id=item_id,
            item_name=item_name,
            quantity=quantity,
        )
    }


@app.get("/items/{item_id}")
def list_item(item_id: int) -> dict[str, dict[str, str]]:
    if not redis_client.hexists(f"item_id:{item_id}", "item_id"):
        raise HTTPException(status_code=404, detail="Item not found")
    else:
        return {"item": redis_client.hgetall(f"item_id:{item_id}")}


# @app.get("/items")
# def list_item() -> dict[str, dict[int, ItemPayload]]:
#     return {"items": grocery_list}


# @app.delete("/items/{item_id}")
# def delete_item(item_id: int):
#     if item_id not in grocery_list:
#         raise HTTPException(status_code=404, detail="Item not found")
#     del grocery_list[item_id]
#     return {"result": "Item deleted"}


# @app.delete("/items/{item_id}/{quantity}")
# def remove_quantity(item_id: int, quantity: int):
#     if item_id not in grocery_list:
#         raise HTTPException(status_code=404, detail="Item not found")
#     if grocery_list[item_id].quantity <= quantity:
#         del grocery_list[item_id]
#         return {"result": "Item deleted"}
#     else:
#         grocery_list[item_id].quantity -= quantity
#         return {"result": f"{quantity} items removed"}
