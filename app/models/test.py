from pydantic import BaseModel
from typing import Optional


class ItemPayload(BaseModel):
    item_id: Optional[int]
    item_name: str
    quantity: int
