from __future__ import annotations

import hashlib
import sys
import requests

from mpdp.config import get_dataset, load_config
from mpdp.s3 import client as s3_client, ensure_bucket, normalize_prefix, put_bytes
from mpdp.timeutil import utc_timestamp


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/ingest_bronze_to_minio.py <dataset_id>")
        return 2

    dataset_id = sys.argv[1]
    cfg = load_config()
    ds = get_dataset(cfg, dataset_id)

    ts = utc_timestamp()
    c = s3_client()

    bucket = ds.lake_bucket
    bronze_prefix = normalize_prefix(ds.bronze_prefix)

    raw_key = f"{bronze_prefix}{dataset_id}__{ts}.{ds.format}"
    meta_key = f"{bronze_prefix}{dataset_id}__{ts}.meta.txt"

    r = requests.get(ds.url, timeout=60)
    r.raise_for_status()
    content = r.content
    digest = sha256_bytes(content)

    ensure_bucket(c, bucket)
    put_bytes(c, bucket, raw_key, content, "application/octet-stream")

    meta = "\n".join(
        [
            f"id={dataset_id}",
            f"url={ds.url}",
            f"retrieved_utc={ts}",
            f"sha256={digest}",
            f"bytes={len(content)}",
        ]
    ) + "\n"
    put_bytes(c, bucket, meta_key, meta.encode("utf-8"), "text/plain; charset=utf-8")

    print("OK: uploaded raw + meta.")
    print(f"Raw:  s3://{bucket}/{raw_key}")
    print(f"Meta: s3://{bucket}/{meta_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())