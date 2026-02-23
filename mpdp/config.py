from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml

DEFAULT_CONFIG_PATH = Path("configs/sources.yaml")


@dataclass(frozen=True)
class Dataset:
    id: str
    owner: str
    source_type: str
    url: str
    format: str
    license: str
    lake_bucket: str
    bronze_prefix: str
    silver_prefix: str
    staging_schema: str
    staging_table: str


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def to_dataset(d: Dict[str, Any]) -> Dataset:
    return Dataset(
        id=d["id"],
        owner=d.get("owner", ""),
        source_type=d.get("source_type", ""),
        url=d.get("url", ""),
        format=(d.get("format") or "bin").lower(),
        license=d.get("license", ""),
        lake_bucket=d.get("lake_bucket", ""),
        bronze_prefix=d.get("bronze_prefix", ""),
        silver_prefix=d.get("silver_prefix", ""),
        staging_schema=d.get("staging_schema", "staging"),
        staging_table=d.get("staging_table", d.get("id", "")),
    )


def get_dataset(cfg: Dict[str, Any], dataset_id: str) -> Dataset:
    datasets = cfg.get("datasets", [])
    if not isinstance(datasets, list):
        raise ValueError("Invalid config: 'datasets' must be a list")

    for d in datasets:
        if d.get("id") == dataset_id:
            return to_dataset(d)

    raise ValueError(f"Dataset id not found in config: {dataset_id}")