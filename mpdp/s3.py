from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from mpdp.settings import get_s3_settings


def normalize_prefix(p: str) -> str:
    p = (p or "").lstrip("/")
    return p if p.endswith("/") else p + "/"


def client():
    s = get_s3_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint_url,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def ensure_bucket(c, bucket: str) -> None:
    try:
        c.head_bucket(Bucket=bucket)
    except ClientError:
        c.create_bucket(Bucket=bucket)


def list_keys(c, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    token: Optional[str] = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = c.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def get_bytes(c, bucket: str, key: str) -> bytes:
    resp = c.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def put_bytes(c, bucket: str, key: str, data: bytes, content_type: str) -> None:
    c.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


def pick_latest_by_lex(
    keys: Iterable[str],
    must_contain: str,
    exclude_suffixes: Tuple[str, ...] = (),
) -> str:
    candidates = [
        k for k in keys
        if must_contain in k and not any(k.endswith(s) for s in exclude_suffixes)
    ]
    if not candidates:
        raise ValueError("No matching objects found for latest pick.")
    return max(candidates)