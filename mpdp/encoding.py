from __future__ import annotations

def decode_bytes_safely(b: bytes) -> str:
    for enc in ("utf-16", "utf-16le", "utf-8-sig", "latin1"):
        try:
            return b.decode(enc)
        except Exception:
            pass
    return b.decode("latin1", errors="replace")


def clean_bom(s: str) -> str:
    return s.replace("\ufeff", "").replace("ÿþ", "").strip()