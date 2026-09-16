"""
Shared HTTP client: retry with exponential backoff + jitter, honoring
Retry-After when present. Both CMS API clients use this instead of calling
httpx directly, so retry behavior is defined once.
"""

from __future__ import annotations

import logging
import random
import time

import httpx

from .config import HTTP, RetryConfig

logger = logging.getLogger("cms_ingestion.http")


class RetryableRequestError(Exception):
    """Raised when a request exhausts all retry attempts."""

    def __init__(self, url: str, last_status: int | None, last_error: str):
        self.url = url
        self.last_status = last_status
        self.last_error = last_error
        super().__init__(f"Exhausted retries for {url}: status={last_status} error={last_error}")


def _sleep_seconds(attempt: int, retry_cfg: RetryConfig, retry_after_header: str | None) -> float:
    """Compute backoff delay: honor Retry-After if the server sent one,
    otherwise exponential backoff with jitter, capped at backoff_max_seconds."""
    if retry_after_header:
        try:
            return min(float(retry_after_header), retry_cfg.backoff_max_seconds)
        except ValueError:
            pass  # Retry-After can be an HTTP date; fall back to backoff below

    base_delay = retry_cfg.backoff_base_seconds * (2 ** (attempt - 1))
    jitter = random.uniform(0, base_delay * 0.25)
    return min(base_delay + jitter, retry_cfg.backoff_max_seconds)


def get_json(
    client: httpx.Client,
    url: str,
    params: dict | None = None,
    retry_cfg: RetryConfig | None = None,
) -> dict:
    """
    GET a URL and return parsed JSON, retrying on timeouts, connection
    errors, and retryable status codes (429/5xx/408) with exponential
    backoff + jitter, capped by max_attempts.
    """
    retry_cfg = retry_cfg or HTTP.retry
    last_status: int | None = None
    last_error = ""

    for attempt in range(1, retry_cfg.max_attempts + 1):
        try:
            response = client.get(url, params=params, timeout=HTTP.timeout_seconds)
            last_status = response.status_code

            if response.status_code == 200:
                return response.json()

            if response.status_code in retry_cfg.retryable_statuses:
                delay = _sleep_seconds(attempt, retry_cfg, response.headers.get("Retry-After"))
                logger.warning(
                    "Retryable status %s on %s (attempt %s/%s) — sleeping %.1fs",
                    response.status_code, url, attempt, retry_cfg.max_attempts, delay,
                )
                time.sleep(delay)
                continue

            # Non-retryable 4xx: fail fast, don't burn attempts.
            response.raise_for_status()

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            last_error = str(exc)
            delay = _sleep_seconds(attempt, retry_cfg, None)
            logger.warning(
                "Network error on %s (attempt %s/%s): %s — sleeping %.1fs",
                url, attempt, retry_cfg.max_attempts, last_error, delay,
            )
            time.sleep(delay)
            continue

    raise RetryableRequestError(url, last_status, last_error or f"HTTP {last_status}")
