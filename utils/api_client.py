import aiohttp
import logging
from typing import AsyncGenerator, Any
from urllib.parse import quote

logger = logging.getLogger('LeakCheck.APIClient')

class APIClient:
    """Handles API communication"""
    BASE_URL = "https://leakcheck.io/api/v2/query/"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def stream_results(self, search_type: str, query: str) -> AsyncGenerator[Any, None]:
        """Stream results from API"""
        await self._ensure_session()
        
        headers = {
            "Accept": "application/json",
            "X-API-Key": self.api_key
        }

        url = f"{self.BASE_URL}{quote(query)}?type={search_type}"
        
        try:
            async with self.session.get(url, headers=headers) as response:
                response.raise_for_status()
                async for chunk in response.content.iter_chunked(4096):
                    try:
                        data = chunk.decode('utf-8')
                        yield data
                    except Exception as e:
                        logger.error(f"Error processing chunk: {e}")
                        continue
        except Exception as e:
            logger.error(f"API request failed: {e}")
            raise

    async def close(self):
        """Close the API client session"""
        if self.session:
            await self.session.close()
            self.session = None
