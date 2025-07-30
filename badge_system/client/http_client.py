import aiohttp

from badge_system.exceptions import HTTPRequestError

class HttpClient:
    def __init__(self, base_url: str, region_url, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.region_url = region_url
        self.token = token
        self.session = aiohttp.ClientSession()

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json"
        }
        if self.token:
            headers["Authorization"] = self.token
        return headers

    async def get(self, endpoint: str, is_regional:bool, params=None):
        url = f"{self.region_url if is_regional else self.base_url}/{endpoint.lstrip('/')}"

        headers = self._get_headers()
        async with self.session.get(url, headers=headers, params=params) as response:
            if response.status != 200 | response.status != 201:
                raise HTTPRequestError(status=response.status, url=str(response.url), reason=response.reason, method=response.method )
            return await response.json()

    async def post(self, endpoint: str, data=None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self._get_headers()
        async with self.session.post(url, headers=headers, json=data) as response:
            if response.status != 200:
                body = await response.text()
                raise HTTPRequestError(response.status, str(response.url), response.reason, body)
            return await response.json()

    async def close(self):
        await self.session.close()
