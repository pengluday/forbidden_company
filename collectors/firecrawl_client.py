from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any


class FirecrawlError(RuntimeError):
    pass


def get_api_key(explicit_key: str | None = None) -> str:
    key = (explicit_key or os.getenv("FIRECRAWL_API_KEY") or "").strip()
    if not key:
        raise FirecrawlError("FIRECRAWL_API_KEY is not set")
    return key


def scrape_url(
    url: str,
    *,
    api_key: str | None = None,
    base_url: str = "https://api.firecrawl.dev",
    formats: list[Any] | None = None,
    actions: list[dict[str, Any]] | None = None,
    only_main_content: bool = True,
    timeout: int = 120,
) -> dict[str, Any]:
    key = get_api_key(api_key)
    endpoint = base_url.rstrip("/") + "/v2/scrape"
    payload: dict[str, Any] = {
        "url": url,
        "formats": formats or ["markdown", "html"],
        "onlyMainContent": only_main_content,
    }
    if actions:
        payload["actions"] = actions

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(endpoint, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {key}")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            raw = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        raise FirecrawlError(f"Firecrawl HTTP {exc.code}: {raw[:500]}") from exc
    except urllib.error.URLError as exc:
        raise FirecrawlError(f"Firecrawl request failed: {exc}") from exc

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise FirecrawlError(f"Firecrawl returned non-JSON content: {raw[:300]}") from exc

    if not isinstance(result, dict):
        raise FirecrawlError("Firecrawl returned unexpected payload")
    if not result.get("success", False):
        message = result.get("error") or result.get("message") or "Firecrawl scrape failed"
        raise FirecrawlError(str(message))
    return result


def search_web(
    query: str,
    *,
    api_key: str | None = None,
    base_url: str = "https://api.firecrawl.dev",
    limit: int = 10,
    country: str = "CN",
    location: str = "Hangzhou,China",
    scrape_options: dict[str, Any] | None = None,
    timeout: int = 120,
) -> dict[str, Any]:
    key = get_api_key(api_key)
    endpoint = base_url.rstrip("/") + "/v2/search"
    payload: dict[str, Any] = {
        "query": query,
        "limit": limit,
        "country": country,
        "location": location,
        "sources": ["web"],
        "ignoreInvalidURLs": True,
    }
    if scrape_options:
        payload["scrapeOptions"] = scrape_options

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(endpoint, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {key}")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            raw = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        raise FirecrawlError(f"Firecrawl HTTP {exc.code}: {raw[:500]}") from exc
    except urllib.error.URLError as exc:
        raise FirecrawlError(f"Firecrawl request failed: {exc}") from exc

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise FirecrawlError(f"Firecrawl returned non-JSON content: {raw[:300]}") from exc

    if not isinstance(result, dict):
        raise FirecrawlError("Firecrawl returned unexpected payload")
    if not result.get("success", False):
        message = result.get("error") or result.get("message") or "Firecrawl search failed"
        raise FirecrawlError(str(message))
    return result
