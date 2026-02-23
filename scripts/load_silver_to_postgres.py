# scripts/load_silver_to_postgres.py
from __future__ import annotations

import os
import sys
from io import BytesIO
from pathlib import Path

import boto3
import polars as pl
import yaml
from botocore.client import Config
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


CONFIG_PATH = Path("configs/sources.yaml")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_dataset(cfg: dict, dataset_id: str) -> dict:
    datasets = cfg.get("datasets", [])
    for d in datasets:
        if d.get("id") == dataset_id:
            return d
    raise ValueError(f"Dataset id not found in config: {dataset_id}")


def normalize_prefix(p: str) -> str:
    p = (p or "").lstrip("/")
    return p if p.endswith("/") else p + "/"


def s3_client():
    load_dotenv()
    endpoint = os.environ["S3_ENDPOINT_URL"]
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def list_objects(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def pick_latest_silver(keys: list[str], dataset_id: str) -> str:
    # matches: <silver_prefix><dataset_id>__YYYYMMDDTHHMMSSZ.parquet
    candidates = [
        k for k in keys
        if f"{dataset_id}__" in k and k.lower().endswith(".parquet")
    ]
    if not candidates:
        raise ValueError("No Silver parquet files found for this dataset.")
    return max(candidates)


def get_object_bytes(client, bucket: str, key: str) -> bytes:
    resp = client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def pg_engine():
    load_dotenv()
    host = os.environ["PG_HOST"]
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ["PG_DATABASE"]
    user = os.environ["PG_USER"]
    pw = os.environ["PG_PASSWORD"]
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(url)


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/load_silver_to_postgres.py <dataset_id>")
        sys.exit(2)

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    bucket = ds["lake_bucket"]
    silver_prefix = normalize_prefix(ds["silver_prefix"])
    staging_schema = ds.get("staging_schema", "staging")
    staging_table = ds["staging_table"]

    # 1) Find latest silver parquet in MinIO
    s3 = s3_client()
    keys = list_objects(s3, bucket, silver_prefix)
    latest_key = pick_latest_silver(keys, dataset_id)

    print(f"Dataset: {dataset_id}")
    print(f"Bucket: {bucket}")
    print(f"Latest Silver key: {latest_key}")
    print(f"Target table: {staging_schema}.{staging_table}")

    # 2) Download parquet bytes
    parquet_bytes = get_object_bytes(s3, bucket, latest_key)

    # 3) Read into Polars
    df = pl.read_parquet(BytesIO(parquet_bytes))
    print(f"Silver rows: {df.height}, cols: {df.width}")

    if df.height == 0:
        raise ValueError("Silver has 0 rows; refusing to load.")

    # 4) Load to Postgres
    engine = pg_engine()

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};"))

    # Use pandas to_sql for simplicity
    pdf = df.to_pandas()
    pdf.to_sql(
        name=staging_table,
        con=engine,
        schema=staging_schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    # 5) Verify count
    with engine.begin() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {staging_schema}.{staging_table};")
        ).scalar_one()

    print("OK: loaded Silver into Postgres.")
    print(f"Loaded from: s3://{bucket}/{latest_key}")
    print(f"Wrote table: {staging_schema}.{staging_table}")
    print(f"Row count in Postgres: {count}")


if __name__ == "__main__":
    main()
    # python scripts\load_silver_to_postgres.py tourism_guides_directory