"""
Look up CMS Certification Numbers (CCNs) for your hospitals.

Reads the raw hospital_general_information pages you already landed in the
Volume, so run the CMS ingestion first:

    from ingestion.run_ingest import main
    main(spark, hospital_files=[])          # CMS datasets only

Then, in a notebook:

    from ingestion.ccn_lookup import find_ccns
    display(find_ccns(spark))

Copy each Facility ID into the matching HospitalFileSpec.ccn in config.py,
as a STRING (CCNs can have leading zeros).
"""

from __future__ import annotations

from . import config

# Case-insensitive fragments of the hospital names you care about.
DEFAULT_PATTERNS = [
    "vanderbilt university medical",
    "saint thomas",
    "williamson medical",
    "specialty surgery",
]


def find_ccns(spark, patterns: list[str] | None = None, state: str = "TN"):
    """Return distinct matching hospitals with their Facility ID (CCN)."""
    from pyspark.sql import functions as F

    patterns = patterns or DEFAULT_PATTERNS
    path = f"{config.VOLUME_ROOT}/hospital_general_information/run_date=*/page_*.json"
    # Each landed page is a JSON array of rows, so multiLine is required.
    df = spark.read.option("multiLine", True).json(path)

    cols = {c.lower(): c for c in df.columns}
    needed = ["facility_id", "facility_name", "state"]
    missing = [c for c in needed if c not in cols]
    if missing:
        raise ValueError(
            f"Expected columns {missing} not found. Actual columns: {df.columns}. "
            "Adjust the names in find_ccns() to match."
        )

    name = F.lower(F.col(cols["facility_name"]))
    matches = name.rlike("|".join(p.lower() for p in patterns))
    keep = [cols[c] for c in ("facility_id", "facility_name", "citytown", "countyparish",
                              "hospital_type", "hospital_ownership") if c in cols]
    return (
        df.filter(matches & (F.col(cols["state"]) == state))
        .select(*keep)
        .distinct()
        .orderBy(cols["facility_name"])
    )
