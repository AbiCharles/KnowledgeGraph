"""Inspect a batch of uploaded CSVs and produce the schema dict the
generator consumes.

For each file:
  - sniff the dialect (delimiter, quote char) via csv.Sniffer
  - read the header row → column names
  - sample the first N rows to infer xsd types per column
  - cache those rows so the generator can seed data.ttl

Stdlib only (csv module). No pandas dep — keeps the runtime image lean
and avoids a multi-MB compiled wheel for a one-shot inspect operation.
"""
from __future__ import annotations
import csv
import io
import re
from datetime import date, datetime
from typing import Iterable

from pipeline.builder.generator import singularise_pascal, _validate_xsd_range


_BOOL_TRUE = {"true", "t", "yes", "y", "1"}
_BOOL_FALSE = {"false", "f", "no", "n", "0"}
_DATE_FMTS = ("%Y-%m-%d",)
_DATETIME_FMTS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
)
# A column gets the typed xsd_type only if at least this fraction of its
# non-empty sampled values parse cleanly as that type. 0.95 = "near-all";
# leaves room for one stray bad row in 20 without us losing the type.
_TYPE_THRESHOLD = 0.95


def _normalise_column_name(raw: str, idx: int) -> str:
    """CSV headers can be 'First Name', 'Order #', etc. Convert to a
    valid OWL property name: lowercase, alphanumerics + underscore.
    Falls back to col<N> if the result is empty or non-alpha-leading."""
    s = (raw or "").strip()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE).strip("_")
    if not s:
        return f"col{idx}"
    if not s[0].isalpha():
        s = f"col_{s}"
    # camelCase the result so it lines up with the rest of the platform's
    # property naming convention. "first_name" → "firstName".
    parts = s.split("_")
    return parts[0].lower() + "".join(p[:1].upper() + p[1:].lower() for p in parts[1:])


def _try_int(v: str) -> bool:
    try:
        int(v)
        return True
    except ValueError:
        return False


def _try_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


def _try_bool(v: str) -> bool:
    return v.lower() in _BOOL_TRUE | _BOOL_FALSE


def _try_date(v: str) -> bool:
    for fmt in _DATE_FMTS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


def _try_datetime(v: str) -> bool:
    for fmt in _DATETIME_FMTS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


def _infer_xsd(values: Iterable[str]) -> str:
    """Classify a column's xsd type from a sample of string values.
    Empty string / None values are excluded from the denominator — a
    column of ['1', '2', None, '3'] is still 100% integer."""
    non_empty = [str(v).strip() for v in values if v is not None and str(v).strip() != ""]
    if not non_empty:
        return "string"
    n = len(non_empty)

    # Boolean takes priority over integer (a column of all '0'/'1' could
    # be either; bool is more semantically meaningful).
    if sum(1 for v in non_empty if _try_bool(v)) / n >= _TYPE_THRESHOLD:
        return "boolean"
    if sum(1 for v in non_empty if _try_int(v)) / n >= _TYPE_THRESHOLD:
        return "integer"
    # Float check excludes pure ints (so we don't downgrade an int column).
    if sum(1 for v in non_empty if _try_float(v) and not _try_int(v)) / n >= _TYPE_THRESHOLD:
        return "decimal"
    if sum(1 for v in non_empty if _try_datetime(v)) / n >= _TYPE_THRESHOLD:
        return "dateTime"
    if sum(1 for v in non_empty if _try_date(v)) / n >= _TYPE_THRESHOLD:
        return "date"
    return "string"


def _detect_primary_key(columns: list[dict], rows: list[dict]) -> str | None:
    """Pick a column to use as the MERGE key. Heuristic: a column whose
    name ends with 'id' (case-insensitive) AND has no duplicates AND no
    null/empty values in the sample. Returns the column name or None
    if nothing qualifies — caller can fall back to the first column."""
    if not columns or not rows:
        return None
    candidates = [
        c for c in columns
        if c["name"].lower().endswith("id") or c["name"].lower() == "id"
    ]
    for c in candidates:
        vals = [r.get(c["name"]) for r in rows]
        if any(v is None or str(v).strip() == "" for v in vals):
            continue
        if len(set(str(v) for v in vals)) == len(vals):
            return c["name"]
    return None


def inspect(
    files: list[tuple[str, bytes]],
    sample_rows: int = 100,
) -> dict:
    """Inspect N CSV files and return the schema dict.

    Args:
      files: list of (filename, file_bytes) tuples — typically straight
             from the upload route's `await UploadFile.read()`.
      sample_rows: how many rows to read for type inference + later
                   data.ttl seeding. Caps high enough to be representative,
                   low enough to be fast on a multi-MB CSV.

    Returns: schema dict matching pipeline.builder.generator's contract,
    with `source_kind: "csv"` and per-table `sample_rows` populated so
    the generator can seed data.ttl.
    """
    if not files:
        raise ValueError("inspect() requires at least one CSV file")
    tables = []
    for filename, blob in files:
        tables.append(_inspect_one(filename, blob, sample_rows))
    return {
        "source_kind": "csv",
        "source_metadata": {"filenames": [f for f, _ in files]},
        "tables": tables,
    }


def _inspect_one(filename: str, blob: bytes, sample_rows: int) -> dict:
    # Decode — best-effort UTF-8 with a friendly error if the file isn't.
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError:
        # Try latin-1 as a fallback (handles many Excel exports).
        try:
            text = blob.decode("latin-1")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{filename}: could not decode as UTF-8 or Latin-1. "
                "Re-save as UTF-8 from your editor."
            ) from exc

    if not text.strip():
        raise ValueError(f"{filename}: file is empty")

    # Sniff dialect from the first 8 KiB (csv.Sniffer's recommendation).
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel  # fallback to comma-quoted

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = reader.fieldnames or []
    if not headers:
        raise ValueError(f"{filename}: no header row detected")

    # Normalise headers + remember the original → normalised mapping so
    # we read cells under the real header name in the row dict.
    normalised = []
    seen = set()
    for i, raw in enumerate(headers):
        name = _normalise_column_name(raw, i)
        # Avoid duplicates after normalisation ("ID" + "id" → "id" twice).
        base = name
        n = 2
        while name in seen:
            name = f"{base}_{n}"
            n += 1
        seen.add(name)
        normalised.append({"raw": raw, "name": name})

    # Read up to sample_rows rows.
    raw_rows: list[dict] = []
    for i, raw_row in enumerate(reader):
        if i >= sample_rows:
            break
        raw_rows.append(raw_row)

    if not raw_rows:
        # Header only, no data — still a valid ontology source (user
        # defines the schema); just emit string columns + 0 sample rows.
        columns = [
            {"name": col["name"], "xsd_type": "string", "nullable": True, "is_pk": False}
            for col in normalised
        ]
        return {
            "name": filename,
            "class_name": singularise_pascal(_strip_ext(filename)),
            "primary_key": None,
            "columns": columns,
            "foreign_keys": [],
            "sample_rows": [],
            "row_count_estimate": 0,
        }

    # Build typed columns: infer xsd from the sample, also build the
    # cleaned sample_rows list (using normalised column names so the
    # generator sees the same key names as the columns metadata).
    columns = []
    cleaned_sample = []
    # Pre-compute a lookup so we can read from the raw row dict by raw header.
    raw_to_norm = {col["raw"]: col["name"] for col in normalised}

    for col in normalised:
        col_values = [r.get(col["raw"]) for r in raw_rows]
        xsd_type = _validate_xsd_range(_infer_xsd(col_values))
        nullable = any(v is None or str(v).strip() == "" for v in col_values)
        columns.append({
            "name": col["name"], "xsd_type": xsd_type,
            "nullable": nullable, "is_pk": False,
        })

    # Build sample_rows under the normalised names.
    for r in raw_rows:
        cleaned_sample.append({raw_to_norm[k]: v for k, v in r.items() if k in raw_to_norm})

    # Detect a primary-key column; mark it on the columns list.
    pk = _detect_primary_key(columns, cleaned_sample)
    if pk:
        for c in columns:
            if c["name"] == pk:
                c["is_pk"] = True
                c["nullable"] = False

    return {
        "name": filename,
        "class_name": singularise_pascal(_strip_ext(filename)),
        "primary_key": pk,
        "columns": columns,
        "foreign_keys": [],
        "sample_rows": cleaned_sample,
        "row_count_estimate": len(raw_rows),
    }


def _strip_ext(name: str) -> str:
    """orders.csv → orders. Doesn't care if there's no extension."""
    if "." in name:
        return name.rsplit(".", 1)[0]
    return name
