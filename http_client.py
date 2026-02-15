"""
Shared async HTTP client helpers with retry/backoff.
"""

import asyncio
import logging
from typing import Optional, Dict, Any

import aiohttp

logger = logging.getLogger(__name__)

_TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}


async def get_json_with_retry(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = 5.0,
    retries: int = 3,
    retry_delay: float = 0.6,
) -> Optional[Dict[str, Any]]:
    last_error = None
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    for attempt in range(retries + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status not in _TRANSIENT_STATUSES:
                        logger.debug("[HTTP] Non-retry status %s for %s", resp.status, url)
                        return None
                    last_error = RuntimeError(f"transient status {resp.status}")
        except Exception as exc:
            last_error = exc
        if attempt < retries:
            await asyncio.sleep(retry_delay * (2 ** attempt))

    if last_error is not None:
        logger.debug("[HTTP] Request failed for %s: %s", url, last_error)
    return None

