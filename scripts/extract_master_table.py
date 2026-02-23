#!/usr/bin/env python3
"""Extract master quantity total table (총괄수량표) from L-003 style PDFs."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable

MASTER_COLUMNS = ["work_name", "spec", "unit", "master_total_qty", "remark"]
MASTER_FILE_PATTERN = re.compile(r"총괄수량표|L-?003", re.IGNORECASE)

COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    "master_total_qty": ["합계", "실제수량", "총수량", "수량", "물량"],
    "remark": ["비고", "참고"],
}


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\r", "\n")).strip()


def is_master_pdf(path: Path) -> bool:
    return bool(MASTER_FILE_PATTERN.search(path.name))


def iter_candidate_tables(page) -> Iterable[list[list[str]]]:
    settings = [
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {
            "vertical_strategy": "lines",
            "horizontal_strategy": "text",
            "intersection_tolerance": 5,
        },
    ]
    seen: set[tuple[tuple[str, ...], ...]] = set()
    for setting in settings:
        tables = page.extract_tables(table_settings=setting) or []
        for table in tables:
            cleaned = tuple(tuple(normalize_text(cell) for cell in row) for row in table if row)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            yield [list(row) for row in cleaned]


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int]]:
    for idx, row in enumerate(table[:6]):
        mapping: dict[str, int] = {}
        for c_idx, cell in enumerate(row):
            for key, options in COLUMN_CANDIDATES.items():
                if key in mapping:
                    continue
                if any(option in cell for option in options):
                    mapping[key] = c_idx
        if "work_name" in mapping and "master_total_qty" in mapping:
            return idx, mapping
    return -1, {}


def choose_master_pdf(pdf_dir: Path) -> Path | None:
    candidates = sorted(p for p in pdf_dir.glob("*.pdf") if is_master_pdf(p))
    return candidates[0] if candidates else None


def extract_master_rows(pdf_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    import pdfplumber  # type: ignore

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if "총괄" not in page_text and "수량표" not in page_text:
                continue

            for table in iter_candidate_tables(page):
                header_idx, header_map = find_header_map(table)
                if header_idx < 0:
                    continue

                carry_work_name = ""
                data_rows = table[header_idx + 1 :]
                for row in data_rows:
                    normalized = [normalize_text(cell) for cell in row]
                    if not any(normalized):
                        continue

                    record = {col: "" for col in MASTER_COLUMNS}
                    for col in MASTER_COLUMNS:
                        idx = header_map.get(col, -1)
                        if idx >= 0 and idx < len(normalized):
                            record[col] = normalized[idx]

                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    if not record["work_name"] and not record["master_total_qty"]:
                        continue
                    rows.append(record)
    return rows


def write_master_csv(rows: list[dict[str, str]], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="총괄수량표 추출")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--out_csv", type=Path, default=Path("output/master_extract.csv"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target = choose_master_pdf(args.pdf_dir)
    rows: list[dict[str, str]] = []
    if target:
        rows = extract_master_rows(target)
    write_master_csv(rows, args.out_csv)
    print(f"Master rows: {len(rows)}")


if __name__ == "__main__":
    main()
