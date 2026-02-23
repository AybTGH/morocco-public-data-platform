# scripts/bronze_to_silver.py
from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
import yaml
from botocore.client import Config
from dotenv import load_dotenv


CONFIG_PATH = Path("configs/sources.yaml")
EXPECTED_COLS = ["nom", "prenom", "ville", "categorie", "langue_de_travail"]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_dataset(cfg: dict, dataset_id: str) -> dict:
    for d in cfg.get("datasets", []):
        if d.get("id") == dataset_id:
            return d
    raise ValueError(f"Dataset id not found: {dataset_id}")


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


def pick_latest_bronze(keys: list[str], dataset_id: str) -> str:
    """
    We name bronze files like:
      <prefix><dataset_id>__YYYYMMDDTHHMMSSZ.<ext>
    So lexicographic max is the latest timestamp.
    """
    candidates = [k for k in keys if f"{dataset_id}__" in k and not k.endswith(".meta.txt")]
    if not candidates:
        raise ValueError("No bronze data files found (only meta or none).")
    return max(candidates)


def get_object_bytes(client, bucket: str, key: str) -> bytes:
    resp = client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def decode_bytes_safely(b: bytes) -> str:
    # The dataset often has UTF-16 BOM (ÿþ symptom), so try utf-16 first
    for enc in ("utf-16", "utf-16le", "utf-8-sig", "latin1"):
        try:
            return b.decode(enc)
        except Exception:
            pass
    return b.decode("latin1", errors="replace")


def clean_bom(s: str) -> str:
    # Remove common BOM artifacts
    return s.replace("\ufeff", "").replace("ÿþ", "").strip()


def parse_tourism_guides(text: str) -> pl.DataFrame:
    """
    Handles the tourism guides export which is often UTF-16 text with
    tab-separated or whitespace-separated columns.
    """
    lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise ValueError("Not enough lines to parse file.")

    # Header is present but messy; we don't rely on it.
    _header = clean_bom(lines[0])

    rows = []
    for ln in lines[1:]:
        ln = clean_bom(ln)

        # Try split by TAB first
        parts = [p.strip() for p in ln.split("\t") if p.strip()]

        # Fallback: split on 2+ spaces
        if len(parts) < 4:
            parts = [p.strip() for p in re.split(r"\s{2,}", ln) if p.strip()]

        if len(parts) < 4:
            # skip junk lines
            continue

        if len(parts) >= 5:
            nom, prenom, ville, categorie, langue = parts[0], parts[1], parts[2], parts[3], parts[4]
        else:
            # 4 columns fallback
            nom_prenom, ville, categorie, langue = parts[0], parts[1], parts[2], parts[3]
            np = nom_prenom.split()
            nom = np[0] if np else ""
            prenom = " ".join(np[1:]) if len(np) > 1 else ""

        rows.append((nom, prenom, ville, categorie, langue))

    df = pl.DataFrame(rows, schema=EXPECTED_COLS, orient="row")

    # Normalize types + trim
    df = df.with_columns(pl.all().cast(pl.Utf8))
    df = df.with_columns([pl.col(c).str.strip_chars().alias(c) for c in EXPECTED_COLS])

    return df


def put_object_bytes(client, bucket: str, key: str, data: bytes, content_type: str):
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/bronze_to_silver.py <dataset_id>")
        sys.exit(2)

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    bucket = ds["lake_bucket"]
    bronze_prefix = normalize_prefix(ds["bronze_prefix"])
    silver_prefix = normalize_prefix(ds["silver_prefix"])

    client = s3_client()

    # 1) find latest bronze file
    bronze_keys = list_objects(client, bucket, bronze_prefix)
    latest_bronze_key = pick_latest_bronze(bronze_keys, dataset_id)

    print(f"Dataset: {dataset_id}")
    print(f"Bucket: {bucket}")
    print(f"Latest bronze key: {latest_bronze_key}")

    # 2) download bronze bytes
    raw_bytes = get_object_bytes(client, bucket, latest_bronze_key)

    # 3) parse -> dataframe
    if dataset_id == "tourism_guides_directory":
        text = decode_bytes_safely(raw_bytes)
        df = parse_tourism_guides(text)
    else:
        raise NotImplementedError(
            f"No parser implemented for dataset_id={dataset_id}. Add a parser function."
        )

    print(f"Parsed rows: {df.height}")
    print(f"Parsed cols: {df.width}")
    print(df.head(5))

    if df.height == 0:
        raise ValueError("Parsed 0 rows. Parsing rules likely wrong for this file version.")

    # 4) write parquet bytes
    ts = utc_timestamp()
    silver_key = f"{silver_prefix}{dataset_id}__{ts}.parquet"

    # 4) write parquet to temporary local file
    ts = utc_timestamp()
    silver_key = f"{silver_prefix}{dataset_id}__{ts}.parquet"

    tmp_path = Path(f".tmp_{dataset_id}__{ts}.parquet")
    df.write_parquet(tmp_path)

    parquet_bytes = tmp_path.read_bytes()
    tmp_path.unlink(missing_ok=True)

    # 5) upload parquet to MinIO
    put_object_bytes(
        client,
        bucket=bucket,
        key=silver_key,
        data=bytes(parquet_bytes),
        content_type="application/octet-stream",
    )

    print("OK: wrote Silver parquet to MinIO.")
    print(f"Silver key: {silver_key}")


if __name__ == "__main__":
    main()
    # python scripts\bronze_to_silver.py tourism_guides_directory