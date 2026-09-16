"""
Client for the data.cms.gov main API (data-api/v1).

Paging model: `size` + `offset` query params, max page size 5000 rows.
Reference: https://data.cms.gov/api-docs
"""

from __future__ import annotations

import logging
from typing import Iterator

import httpx

from .config import CMS_DATA_API_BASE, HTTP
from .http_client import get_json

logger = logging.getLogger("cms_ingestion.cms_data_api")


def fetch_dataset_pages(
    client: httpx.Client,
    dataset_uuid: str,
    filters: dict | None = None,
    page_size: int | None = None,
) -> Iterator[tuple[int, list[dict]]]:
    """
    Yields (page_number, rows) for a data.cms.gov dataset, paging with
    size/offset until a page returns fewer rows than requested (end of data).

    `filters` maps column -> exact-match value and is translated into the
    API's filter[COLUMN]=VALUE query param. For more advanced filtering
    (CONTAINS, ranges) build the param dict yourself and pass it via
    `extra_params` in a future revision — the simple case covers most of
    what a course-scoped project needs.
    """
    page_size = page_size or HTTP.page_size
    offset = 0
    page_number = 0

    while True:
        params = {"size": page_size, "offset": offset}
        if filters:
            for column, value in filters.items():
                params[f"filter[{column}]"] = value

        url = f"{CMS_DATA_API_BASE}/dataset/{dataset_uuid}/data"
        rows = get_json(client, url, params=params)

        page_number += 1
        row_count = len(rows) if isinstance(rows, list) else 0
        logger.info("dataset=%s page=%s offset=%s rows=%s", dataset_uuid, page_number, offset, row_count)

        yield page_number, rows

        if row_count < page_size:
            break  # short page = last page
        offset += page_size


def fetch_dataset_metadata(client: httpx.Client, dataset_uuid: str) -> dict:
    """Fetch dataset-level metadata (column list, row count, last-modified)."""
    url = f"{CMS_DATA_API_BASE}/dataset/{dataset_uuid}"
    return get_json(client, url)
