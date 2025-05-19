# src/routes/search_ro.py

from fastapi import APIRouter

router = APIRouter()


@router.get("/search_ro", tags=["search_ro"])
async def search_ro():
    return {"message": "Search for RO data"}
