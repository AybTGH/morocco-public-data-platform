from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class PGSettings:
    host: str
    port: str
    database: str
    user: str
    password: str


def load_env() -> None:
    # Safe to call multiple times
    load_dotenv()


def get_s3_settings() -> S3Settings:
    load_env()
    return S3Settings(
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
    )


def get_pg_settings() -> PGSettings:
    load_env()
    return PGSettings(
        host=os.environ["PG_HOST"],
        port=os.environ.get("PG_PORT", "5432"),
        database=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )