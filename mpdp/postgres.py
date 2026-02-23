from __future__ import annotations

from sqlalchemy import create_engine
from mpdp.settings import get_pg_settings

def engine():
    s = get_pg_settings()
    url = f"postgresql+psycopg2://{s.user}:{s.password}@{s.host}:{s.port}/{s.database}"
    return create_engine(url)