# scripts/ingest_bronze_to_minio.py
from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
import yaml
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


CONFIG_PATH = Path("configs/sources.yaml")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_dataset(cfg: dict, dataset_id: str) -> dict:
    datasets = cfg.get("datasets", [])
    if not isinstance(datasets, list):
        raise ValueError("Invalid config: 'datasets' must be a list")
    for d in datasets:
        if d.get("id") == dataset_id:
            return d
    raise ValueError(f"Dataset id not found in config: {dataset_id}")


def download_file(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def s3_client():
    load_dotenv()

    endpoint = os.environ["S3_ENDPOINT_URL"]
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]

    # MinIO is S3-compatible; force path-style addressing.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # If bucket doesn't exist or not accessible, try to create it.
        # MinIO typically allows create without region config.
        if code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=bucket)
            return
        # Some setups return 403 when bucket doesn't exist but access is restricted
        # (rare for local MinIO). We'll try create once.
        try:
            client.create_bucket(Bucket=bucket)
            return
        except Exception:
            raise


def normalize_prefix(p: str) -> str:
    p = (p or "").lstrip("/")
    return p if p.endswith("/") else p + "/"


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/ingest_bronze_to_minio.py <dataset_id>")
        sys.exit(2)

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    url = ds["url"]
    fmt = (ds.get("format") or "bin").lower()
    bucket = ds["lake_bucket"]
    bronze_prefix = normalize_prefix(ds["bronze_prefix"])

    ts = utc_timestamp()
    raw_key = f"{bronze_prefix}{dataset_id}__{ts}.{fmt}"
    meta_key = f"{bronze_prefix}{dataset_id}__{ts}.meta.txt"

    print(f"Dataset: {dataset_id}")
    print(f"URL: {url}")
    print(f"Bucket: {bucket}")
    print(f"Bronze prefix: {bronze_prefix}")
    print(f"Raw key: {raw_key}")
    print(f"Meta key: {meta_key}")

    # 1) Download
    content = download_file(url)
    digest = sha256_bytes(content)

    # 2) Upload to MinIO
    client = s3_client()
    ensure_bucket(client, bucket)

    client.put_object(
        Bucket=bucket,
        Key=raw_key,
        Body=content,
        ContentType="application/octet-stream",
    )

    meta = "\n".join(
        [
            f"id={dataset_id}",
            f"url={url}",
            f"retrieved_utc={ts}",
            f"sha256={digest}",
            f"bytes={len(content)}",
        ]
    ) + "\n"

    client.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=meta.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )

    print("OK: uploaded raw + meta to MinIO.")
    print(f"sha256: {digest}")
    print(f"bytes: {len(content)}")


if __name__ == "__main__":
    main()
    # python scripts\ingest_bronze_to_minio.py tourism_guides_directory