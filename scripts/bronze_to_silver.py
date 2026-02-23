from __future__ import annotations

import sys

from mpdp.config import get_dataset, load_config
from mpdp.encoding import decode_bytes_safely
from mpdp.io import df_to_parquet_bytes
from mpdp.parsers import get_parser
from mpdp.s3 import client as s3_client, get_bytes, list_keys, normalize_prefix, pick_latest_by_lex, put_bytes
from mpdp.timeutil import utc_timestamp


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/bronze_to_silver.py <dataset_id>")
        return 2

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    c = s3_client()

    bucket = ds.lake_bucket
    bronze_prefix = normalize_prefix(ds.bronze_prefix)
    silver_prefix = normalize_prefix(ds.silver_prefix)

    keys = list_keys(c, bucket, bronze_prefix)
    latest_bronze_key = pick_latest_by_lex(
        keys,
        must_contain=f"{dataset_id}__",
        exclude_suffixes=(".meta.txt",),
    )

    raw = get_bytes(c, bucket, latest_bronze_key)
    text = decode_bytes_safely(raw)

    parser = get_parser(dataset_id)
    df = parser(text)

    if df.height == 0:
        raise ValueError("Parsed 0 rows; refusing to write Silver.")

    ts = utc_timestamp()
    silver_key = f"{silver_prefix}{dataset_id}__{ts}.parquet"
    parquet_bytes = df_to_parquet_bytes(df, tmp_name=f".tmp_{dataset_id}__{ts}.parquet")

    put_bytes(c, bucket, silver_key, parquet_bytes, "application/octet-stream")

    print("OK: wrote Silver parquet.")
    print(f"Bronze: s3://{bucket}/{latest_bronze_key}")
    print(f"Silver:  s3://{bucket}/{silver_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# python -m scripts.show_sources tourism_guides_directory
# python -m scripts.bronze_to_silver tourism_guides_directory
# python -m scripts.ingest_bronze_to_minio tourism_guides_directory
# python -m scripts.load_silver_to_postgres tourism_guides_directory