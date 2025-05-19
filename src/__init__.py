# src/__init__.py

from fastapi import FastAPI
from src.routes.search_ro import router as search_ro_router

app = FastAPI()

app.include_router(search_ro_router)
