from __future__ import annotations
from typing import Dict, List

REQUIRED_KEYS = [
    "id",
    "owner",
    "source_type",
    "url",
    "format",
    "license",
    "lake_bucket",
    "bronze_prefix",
    "silver_prefix",
    "staging_schema",
    "staging_table",
]


def validate_dataset_contract(ds: Dict) -> List[str]:
    missing = [k for k in REQUIRED_KEYS if not ds.get(k)]
    problems: List[str] = []

    # prefix sanity checks
    for k in ("bronze_prefix", "silver_prefix"):
        v = ds.get(k)
        if v and not v.endswith("/"):
            problems.append(f"{k} should end with '/' (got: {v})")

    bp = ds.get("bronze_prefix", "")
    sp = ds.get("silver_prefix", "")
    if bp and not bp.startswith("bronze/"):
        problems.append(f"bronze_prefix should start with 'bronze/' (got: {bp})")
    if sp and not sp.startswith("silver/"):
        problems.append(f"silver_prefix should start with 'silver/' (got: {sp})")

    if missing:
        problems.insert(0, f"missing keys: {', '.join(missing)}")

    return problems