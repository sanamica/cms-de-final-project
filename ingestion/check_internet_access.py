"""
Outbound internet check for Databricks Free Edition.

Free Edition limits serverless compute to a set of trusted domains, so this
tells you which of YOUR sources are reachable before you build on them.
Run it from a Databricks notebook (it must run there, not on your laptop):

    from ingestion.check_internet_access import main
    main()

It only reads the first few bytes of each URL, so large price files are safe.
Add extra URLs to EXTRA_URLS below, or fill config.HOSPITAL_FILES and they are
picked up automatically.
"""

from __future__ import annotations

import time

import httpx

from . import config

# pypi.org is reachable on Free Edition, so it is the control: if the control
# passes and a target fails, that target's domain is being blocked.
CONTROL_URL = "https://pypi.org/simple/httpx/"

EXTRA_URLS: dict[str, str] = {
    # "hospital: example": "https://example.org/path/to/standard-charges.csv",
}

USER_AGENTS = {
    "project UA": "nss-de-final-project/1.0",
    "browser-like UA": "Mozilla/5.0 (compatible; nss-de-final-project/1.0)",
}


def _targets() -> dict[str, str]:
    targets = {
        "control (pypi.org)": CONTROL_URL,
        "CMS data.cms.gov": "https://data.cms.gov/",
        "CMS Provider Data Catalog API": (
            f"{config.CMS_PROVIDER_DATA_API_BASE}/metastore/schemas/dataset/items/xubh-q36u"
        ),
    }
    for spec in config.HOSPITAL_FILES:
        targets[f"hospital: {spec.key}"] = spec.url
    targets.update(EXTRA_URLS)
    return targets


def check(url: str, user_agent: str) -> str:
    """Return a one-line verdict for one URL and user agent."""
    start = time.time()
    try:
        with httpx.Client(headers={"User-Agent": user_agent}, follow_redirects=True) as client:
            with client.stream("GET", url, timeout=20.0) as resp:
                first = next(resp.iter_bytes(512), b"")
                ctype = resp.headers.get("content-type", "?")
                took = time.time() - start
                if resp.status_code == 200:
                    looks_html = first.lstrip()[:15].lower().startswith((b"<!doctype", b"<html"))
                    note = " (HTML page)" if looks_html else ""
                    return f"OK        HTTP 200, {ctype}{note}, {took:.1f}s"
                if resp.status_code in (401, 403):
                    return f"REJECTED  HTTP {resp.status_code}: reachable, but the site refused this client"
                return f"REACHABLE HTTP {resp.status_code}, {ctype}"
    except httpx.HTTPError as exc:
        return f"BLOCKED?  {type(exc).__name__}: {str(exc)[:80]} ({time.time() - start:.1f}s)"


def main() -> dict[str, dict[str, str]]:
    results: dict[str, dict[str, str]] = {}
    for label, url in _targets().items():
        results[label] = {ua: check(url, agent) for ua, agent in USER_AGENTS.items()}

    for label, per_ua in results.items():
        print(f"\n{label}\n  {_targets()[label]}")
        for ua, verdict in per_ua.items():
            print(f"  [{ua:15}] {verdict}")

    control = results["control (pypi.org)"]["project UA"]
    print("\n--- How to read this ---")
    if not control.startswith("OK"):
        print("The control failed, so this notebook has no general outbound access. "
              "Check that you are running on Databricks serverless, not locally.")
    else:
        print("Control OK. Any BLOCKED? line above is a domain Free Edition is not allowing. "
              "REJECTED means the site is reachable and just dislikes the client; "
              "try the browser-like UA in config.FILES.user_agent.")
    return results


if __name__ == "__main__":
    main()
