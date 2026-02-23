from __future__ import annotations

import sys
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table


CONFIG_PATH = Path("configs/sources.yaml")

REQUIRED_KEYS = [
    "id",
    "owner",
    "source_type",
    "url",
    "format",
    "license",
    "lake_bucket",
    "bronze_prefix",
    "silver_prefix",
    "staging_schema",
    "staging_table",
]


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def validate_dataset(ds: dict) -> list[str]:
    missing = [k for k in REQUIRED_KEYS if not ds.get(k)]
    problems = []

    #prefix sanity checks
    for k in ("bronze_prefix", "silver_prefix"):
        v = ds.get(k)
        if v and not v.endswith("/"):
            problems.append(f"{k} should end with '/' (got: {v})")

    bp = ds.get("bronze_prefix", "")
    sp = ds.get("silver_prefix", "")
    if bp and not bp.startswith("bronze/"):
        problems.append(f"bronze_prefix should start with 'bronze/' (got: {bp})")
    if sp and not sp.startswith("silver/"):
        problems.append(f"silver_prefix should start with 'silver/' (got: {sp})")

    if missing:
        problems.insert(0, f"missing keys: {', '.join(missing)}")

    return problems


def main():
    console = Console()

    if not CONFIG_PATH.exists():
        console.print(f"[red]Error:[/red] {CONFIG_PATH} not found.")
        sys.exit(2)

    try:
        cfg = load_config()
    except yaml.YAMLError as e:
        console.print(f"[red]YAML parsing error:[/red] {e}")
        sys.exit(2)

    datasets = cfg.get("datasets", [])
    if not isinstance(datasets, list) or len(datasets) == 0:
        console.print("[yellow]No datasets found in configs/sources.yaml[/yellow]")
        sys.exit(1)

    # Optional: filter by id
    filter_id = sys.argv[1] if len(sys.argv) > 1 else None
    if filter_id:
        datasets = [d for d in datasets if d.get("id") == filter_id]
        if not datasets:
            console.print(f"[red]Dataset id not found:[/red] {filter_id}")
            sys.exit(1)

    table = Table(title="Datasets (contract view)")
    table.add_column("id", style="bold")
    table.add_column("format")
    table.add_column("bucket")
    table.add_column("bronze_prefix")
    table.add_column("silver_prefix")
    table.add_column("staging")
    table.add_column("status")

    any_errors = False

    for ds in datasets:
        problems = validate_dataset(ds)
        status = "[green]OK[/green]" if not problems else "[red]INVALID[/red]"
        if problems:
            any_errors = True

        table.add_row(
            str(ds.get("id", "")),
            str(ds.get("format", "")),
            str(ds.get("lake_bucket", "")),
            str(ds.get("bronze_prefix", "")),
            str(ds.get("silver_prefix", "")),
            f"{ds.get('staging_schema','')}.{ds.get('staging_table','')}",
            status,
        )

    console.print(table)

    #print problems after the table
    for ds in datasets:
        problems = validate_dataset(ds)
        if problems:
            console.print(f"\n[bold red]Problems for dataset:[/bold red] {ds.get('id')}")
            for p in problems:
                console.print(f" - [red]{p}[/red]")

    if any_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()