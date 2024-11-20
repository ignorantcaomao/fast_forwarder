from fastapi import FastAPI
from app.core.config import settings

app = FastAPI(
    debug=True,
    title=settings.PROJECT_NAME
)

