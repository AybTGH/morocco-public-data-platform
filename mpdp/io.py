from __future__ import annotations

from pathlib import Path
import polars as pl

def df_to_parquet_bytes(df: pl.DataFrame, tmp_name: str) -> bytes:
    tmp = Path(tmp_name)
    df.write_parquet(tmp)
    b = tmp.read_bytes()
    tmp.unlink(missing_ok=True)
    return b