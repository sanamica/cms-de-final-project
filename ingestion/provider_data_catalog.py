"""
Client for the CMS Provider Data Catalog API (data.cms.gov/provider-data).

This is a genuinely separate system from the main data.cms.gov API — different
base path, different metastore, different practical page-size limits. Don't
reuse cms_data_api.py's assumptions here.

Dataset identifiers (short alphanumeric codes, not UUIDs) come from:
https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
"""

from __future__ import annotations

import logging
from typing import Iterator

import httpx

from .config import CMS_PROVIDER_DATA_API_BASE, HTTP
from .http_client import get_json

logger = logging.getLogger("cms_ingestion.provider_data_catalog")


def list_datasets(client: httpx.Client) -> list[dict]:
    """
    Full catalog listing — title, identifier, modified date, distribution
    (download) info for every Provider Data Catalog dataset. Useful for a
    one-time discovery run to find the dataset IDs you want, rather than
    hardcoding UUIDs you found by hand.
    """
    url = f"{CMS_PROVIDER_DATA_API_BASE}/metastore/schemas/dataset/items"
    return get_json(client, url)


def fetch_dataset_pages(
    client: httpx.Client,
    dataset_id: str,
    page_size: int | None = None,
) -> Iterator[tuple[int, list[dict]]]:
    """
    Yields (page_number, rows) for a Provider Data Catalog dataset.

    The datastore query endpoint pages with `limit` + `offset` (not
    `size` + `offset` like the main API) and, in practice, is more reliable
    kept to a few hundred rows per page than the main API's 5000-row max —
    large national datasets (e.g. all-hospital HCAHPS) are more prone to
    timeouts at bigger page sizes.
    """
    page_size = page_size or HTTP.provider_page_size
    offset = 0
    page_number = 0

    while True:
        url = f"{CMS_PROVIDER_DATA_API_BASE}/datastore/query/{dataset_id}/0"
        params = {"limit": page_size, "offset": offset}
        payload = get_json(client, url, params=params)

        # This endpoint wraps rows in a "results" key rather than returning
        # a bare list — unlike the main data.cms.gov API. Normalize here so
        # callers don't need to know the difference.
        rows = payload.get("results", []) if isinstance(payload, dict) else payload

        page_number += 1
        row_count = len(rows)
        logger.info("dataset=%s page=%s offset=%s rows=%s", dataset_id, page_number, offset, row_count)

        yield page_number, rows

        if row_count < page_size:
            break
        offset += page_size
