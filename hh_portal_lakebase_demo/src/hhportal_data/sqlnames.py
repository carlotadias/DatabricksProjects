"""Identifier quoting for Unity Catalog names. All names come from bundle variables."""

from __future__ import annotations

import re

_SAFE = re.compile(r"^[A-Za-z0-9_-]+$")


def quote_part(part: str) -> str:
    if not _SAFE.match(part or ""):
        raise ValueError(f"unsafe identifier: {part!r}")
    return f"`{part}`"


def fqn(catalog: str, schema: str, name: str) -> str:
    return f"{quote_part(catalog)}.{quote_part(schema)}.{quote_part(name)}"
