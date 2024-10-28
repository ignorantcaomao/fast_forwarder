from fastapi import FastAPI
from config import settings
app = FastAPI(
    debug=True,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    title=settings.PROJECT_NAME
)
