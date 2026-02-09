# File: utils/api_utils.py
import aiohttp
from urllib.parse import quote
from aiocache import cached

@cached(ttl=3600)
async def perform_check(api_key: str, base_url: str, search_type: str, query: str) -> dict:
    headers = {
        "Accept": "application/json",
        "X-API-Key": api_key
    }

    encoded_query = quote(query)
    url = f"{base_url}{encoded_query}?type={search_type}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {"success": False, "error": f"API error: {response.status} - {await response.text()}"}