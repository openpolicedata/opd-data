"""Generate stable primary keys for the OPD source table."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Optional


SOURCE_TABLE_ID = "source_table_id"
KEY_COLUMNS = ("State", "SourceName", "Agency", "TableType", "Year", "URL", "dataset_id")
ID_PREFIX = "ost_"
ID_LENGTH = 16


def _canonical_value(value: Optional[str]) -> str:
    if value is None:
        return ""

    value = value.strip()
    if not value:
        return ""

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value

    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


def build_source_table_id(row: dict[str, str]) -> str:
    key = "\x1f".join(_canonical_value(row.get(column)) for column in KEY_COLUMNS)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:ID_LENGTH]
    return f"{ID_PREFIX}{digest}"


def _fieldnames(fieldnames: list[str]) -> list[str]:
    fieldnames = [field for field in fieldnames if field != SOURCE_TABLE_ID]
    if "Year" not in fieldnames:
        raise ValueError("Source table is missing required Year column")

    insert_at = fieldnames.index("Year") + 1
    fieldnames.insert(insert_at, SOURCE_TABLE_ID)
    return fieldnames


def add_source_table_ids(source_table: Path) -> tuple[list[str], list[dict[str, str]]]:
    with source_table.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{source_table} does not contain a header row")

        missing = [column for column in KEY_COLUMNS if column not in reader.fieldnames]
        if missing:
            raise ValueError(f"{source_table} is missing key column(s): {', '.join(missing)}")

        fieldnames = _fieldnames(reader.fieldnames)
        rows = []
        for row in reader:
            row[SOURCE_TABLE_ID] = build_source_table_id(row)
            rows.append(row)

    id_counts = Counter(row[SOURCE_TABLE_ID] for row in rows)
    duplicates = sorted(source_id for source_id, count in id_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate {SOURCE_TABLE_ID} value(s): {', '.join(duplicates)}")

    return fieldnames, rows


def write_source_table(source_table: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with source_table.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source_table",
        nargs="?",
        default=Path("opd_source_table.csv"),
        type=Path,
        help="Path to opd_source_table.csv",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate that source_table_id values are current without rewriting the file.",
    )
    args = parser.parse_args()

    source_table = args.source_table
    fieldnames, rows = add_source_table_ids(source_table)

    if args.check:
        with source_table.open(newline="", encoding="utf-8") as handle:
            current = list(csv.DictReader(handle))

        if len(current) != len(rows):
            raise ValueError("Source table row count changed while validating IDs")

        stale_rows = []
        for index, (current_row, expected_row) in enumerate(zip(current, rows)):
            if current_row.get(SOURCE_TABLE_ID) != expected_row[SOURCE_TABLE_ID]:
                stale_rows.append(index + 2)
        if stale_rows:
            raise ValueError(
                f"{SOURCE_TABLE_ID} is stale on row(s): "
                + ", ".join(str(row_number) for row_number in stale_rows[:20])
            )

        return 0

    write_source_table(source_table, fieldnames, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
