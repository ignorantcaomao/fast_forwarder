from app.api.api import apirouter
from fastapi import APIRouter

router = APIRouter()

router.include_router(apirouter)
