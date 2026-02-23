# morocco-public-data-platform
this project aims to get data from open sources form the government and transform it to be useful then put it in a dashboard 

Overview: 
- Pipeline: ingest --> store raw data --> transform --> publish 
- Sources:
- Contracts: expected schema + naming + primary keys + partitionning + quality rules
- Ops: scheduming + logging + retries + versionning + monitoring 
- Stack technique: ...

## architecture
""" 
morocco-public-data-platform/
├─ configs/
│  └─ sources.yaml                 # dataset contracts (YAML)
├─ scripts/                        # thin CLIs only (no shared logic)
│  ├─ show_sources.py              # display + validate contracts
│  ├─ ingest_bronze.py             # download -> bronze (MinIO) + metadata
│  ├─ bronze_to_silver.py          # bronze latest -> parse -> silver parquet (MinIO)
│  └─ load_silver_to_postgres.py   # silver latest -> Postgres staging
├─ mpdp/                           # reusable internal package (shared code)
│  ├─ __init__.py
│  ├─ settings.py                  # env vars access + defaults
│  ├─ timeutil.py                  # utc timestamps
│  ├─ config.py                    # load/validate contracts + Dataset dataclass
│  ├─ s3.py                        # MinIO client + list/get/put helpers
│  ├─ encoding.py                  # decode + BOM cleanup
│  ├─ io.py                        # temp parquet helpers (optional)
│  ├─ postgres.py                  # SQLAlchemy engine helper
│  ├─ validators/
│  │  ├─ __init__.py
│  │  └─ contract.py               # contract validation (used by show_sources + runtime)
│  └─ parsers/
│     ├─ __init__.py               # registry: dataset_id -> parser
│     └─ tourism_guides.py         # parse() + expected schema for that dataset
├─ .env                            # local secrets (not committed)
├─ .gitignore
├─ requirements.txt                # or pyproject.toml
└─ README.md
"""
### activate env
.venv\Scripts\activate