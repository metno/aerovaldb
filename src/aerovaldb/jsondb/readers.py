from ..utils import async_and_sync
import aiofile
import async_lru
from pathlib import Path


@async_and_sync
async def uncached_load_json(file_path: str | Path) -> str:
    async with aiofile.async_open(file_path, "r") as f:
        return await f.read()


@async_and_sync
@async_lru.alru_cache(maxsize=64)
async def cached_load_json(file_path: str | Path) -> str:
    return await uncached_load_json(file_path)
