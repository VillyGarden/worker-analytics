import httpx
from typing import AsyncIterator, Dict, Any, Optional
from .config import settings

HEADERS = {
    "Authorization": f"Bearer {settings.MS_API_TOKEN}",
    "Accept": "application/json;charset=utf-8",
    "Accept-Encoding": "gzip",
    "User-Agent": "worker-analytics/1.0"
}

BASE = settings.MS_BASE_URL.rstrip("/")

class MSClient:
    def __init__(self, timeout: float = 30.0):
        self._client = httpx.AsyncClient(timeout=timeout, headers=HEADERS, base_url=BASE)

    async def close(self):
        await self._client.aclose()

    async def paged(self, path: str, limit: int = 1000, params: Optional[Dict[str, Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        offset = 0
        params = dict(params or {})
        while True:
            q = params | {"limit": limit, "offset": offset}
            r = await self._client.get(path, params=q)
            r.raise_for_status()
            data = r.json()
            rows = data.get("rows", [])
            for row in rows:
                yield row
            if len(rows) < limit:
                break
            offset += limit

    async def get_stores(self) -> AsyncIterator[Dict[str, Any]]:
        async for row in self.paged("/entity/store"):
            yield row
