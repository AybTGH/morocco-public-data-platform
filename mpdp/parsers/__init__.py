from __future__ import annotations

from typing import Callable, Dict
import polars as pl

from mpdp.parsers.tourism_guides import parse as parse_tourism_guides

PARSERS: Dict[str, Callable[[str], pl.DataFrame]] = {
    "tourism_guides_directory": parse_tourism_guides,
}

def get_parser(dataset_id: str) -> Callable[[str], pl.DataFrame]:
    try:
        return PARSERS[dataset_id]
    except KeyError:
        raise NotImplementedError(f"No parser registered for dataset_id={dataset_id}")