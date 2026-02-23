from __future__ import annotations

import re
import polars as pl
from mpdp.encoding import clean_bom

EXPECTED_COLS = ["nom", "prenom", "ville", "categorie", "langue_de_travail"]

def parse(text: str) -> pl.DataFrame:
    lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise ValueError("Not enough lines to parse file.")

    _header = clean_bom(lines[0])  # debug only

    rows = []
    for ln in lines[1:]:
        ln = clean_bom(ln)

        parts = [p.strip() for p in ln.split("\t") if p.strip()]
        if len(parts) < 4:
            parts = [p.strip() for p in re.split(r"\s{2,}", ln) if p.strip()]

        if len(parts) < 4:
            continue

        if len(parts) >= 5:
            nom, prenom, ville, categorie, langue = parts[0], parts[1], parts[2], parts[3], parts[4]
        else:
            nom_prenom, ville, categorie, langue = parts[0], parts[1], parts[2], parts[3]
            np = nom_prenom.split()
            nom = np[0] if np else ""
            prenom = " ".join(np[1:]) if len(np) > 1 else ""

        rows.append((nom, prenom, ville, categorie, langue))

    df = pl.DataFrame(rows, schema=EXPECTED_COLS, orient="row")
    df = df.with_columns(pl.all().cast(pl.Utf8))
    df = df.with_columns([pl.col(c).str.strip_chars().alias(c) for c in EXPECTED_COLS])
    return df