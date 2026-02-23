from __future__ import annotations

import sys
from rich.console import Console
from rich.table import Table

from mpdp.config import load_config
from mpdp.validators.contract import validate_dataset_contract


def main() -> int:
    console = Console()

    try:
        cfg = load_config()
    except Exception as e:
        console.print(f"[red]Error loading config:[/red] {e}")
        return 2

    datasets = cfg.get("datasets", [])
    if not isinstance(datasets, list) or len(datasets) == 0:
        console.print("[yellow]No datasets found in configs/sources.yaml[/yellow]")
        return 1

    filter_id = sys.argv[1] if len(sys.argv) > 1 else None
    if filter_id:
        datasets = [d for d in datasets if d.get("id") == filter_id]
        if not datasets:
            console.print(f"[red]Dataset id not found:[/red] {filter_id}")
            return 1

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
        problems = validate_dataset_contract(ds)
        status = "[green]OK[/green]" if not problems else "[red]INVALID[/red]"
        any_errors = any_errors or bool(problems)

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

    for ds in datasets:
        problems = validate_dataset_contract(ds)
        if problems:
            console.print(f"\n[bold red]Problems for dataset:[/bold red] {ds.get('id')}")
            for p in problems:
                console.print(f" - [red]{p}[/red]")

    return 1 if any_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())