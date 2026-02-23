from __future__ import annotations

import sys
from io import BytesIO

import polars as pl
from sqlalchemy import text

from mpdp.config import get_dataset, load_config
from mpdp.postgres import engine as pg_engine
from mpdp.s3 import client as s3_client, get_bytes, list_keys, normalize_prefix, pick_latest_by_lex


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/load_silver_to_postgres.py <dataset_id>")
        return 2

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    c = s3_client()
    bucket = ds.lake_bucket
    silver_prefix = normalize_prefix(ds.silver_prefix)

    keys = list_keys(c, bucket, silver_prefix)
    latest_silver_key = pick_latest_by_lex(
        keys,
        must_contain=f"{dataset_id}__",
        exclude_suffixes=(),
    )

    parquet_bytes = get_bytes(c, bucket, latest_silver_key)
    df = pl.read_parquet(BytesIO(parquet_bytes))
    if df.height == 0:
        raise ValueError("Silver has 0 rows; refusing to load.")

    engine = pg_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {ds.staging_schema};"))

    df.to_pandas().to_sql(
        name=ds.staging_table,
        con=engine,
        schema=ds.staging_schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    with engine.begin() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {ds.staging_schema}.{ds.staging_table};")
        ).scalar_one()

    print("OK: loaded Silver into Postgres.")
    print(f"From: s3://{bucket}/{latest_silver_key}")
    print(f"To:   {ds.staging_schema}.{ds.staging_table}")
    print(f"Rows: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())