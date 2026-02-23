#!/usr/bin/env python3
"""Extract plan quantity tables (수량표) from landscape PDFs."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

DEFAULT_COLUMNS = [
    "source_pdf",
    "source_page",
    "floor_label",
    "trade_category",
    "work_name",
    "spec",
    "unit",
    "qty",
    "remark",
]

TABLE_TITLE_PATTERN = re.compile(r"수량표")
SUMMARY_FILE_PATTERN = re.compile(r"총괄수량표|L-?003", re.IGNORECASE)

FLOOR_PATTERNS = [
    re.compile(r"(옥상층?)"),
    re.compile(r"(지상\s*\d+\s*층)"),
    re.compile(r"(지하\s*\d+\s*층)"),
    re.compile(r"(B\s*\d+\s*층?)", re.IGNORECASE),
    re.compile(r"(?<!지상)(?<!지하)(\b\d+\s*층\b)"),
    re.compile(r"(지상층)"),
]

TRADE_KEYWORDS = {
    "식재지반": ["식재지반"],
    "식재": ["식재"],
    "시설물": ["시설물"],
    "포장": ["포장"],
    "관수": ["관수"],
    "조명": ["조명", "조명기기"],
    "배수": ["배수"],
}

COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    "qty": ["수량", "물량", "량"],
    "remark": ["비고", "참고"],
}


@dataclass
class ExtractLog:
    source_pdf: str
    source_page: int
    table_title: str
    row_count: int
    fail_reason: str


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", "\n")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_key_text(value: str) -> str:
    value = normalize_text(value)
    return re.sub(r"\s+", " ", value).strip()


def detect_floor(*chunks: str) -> str:
    source = " ".join(chunks)
    for pattern in FLOOR_PATTERNS:
        match = pattern.search(source)
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return "미지정"


def detect_trade_category(*chunks: str) -> str:
    text = " ".join(chunks)
    for category, keywords in TRADE_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "기타"


def is_master_pdf(path: Path) -> bool:
    return bool(SUMMARY_FILE_PATTERN.search(path.name))


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


def find_table_title(page_text: str) -> str:
    for line in page_text.splitlines():
        line = normalize_text(line)
        if TABLE_TITLE_PATTERN.search(line):
            return line
    return ""


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int]]:
    for idx, row in enumerate(table[:5]):
        mapping: dict[str, int] = {}
        for c_idx, cell in enumerate(row):
            cell_norm = normalize_text(cell)
            for target, options in COLUMN_CANDIDATES.items():
                if target in mapping:
                    continue
                if any(option in cell_norm for option in options):
                    mapping[target] = c_idx
        if "work_name" in mapping and "qty" in mapping:
            return idx, mapping
    return -1, {}


def row_to_record(
    row: list[str],
    header_map: dict[str, int],
    source_pdf: str,
    source_page: int,
    floor_label: str,
    trade_category: str,
) -> dict[str, str]:
    record = {col: "" for col in DEFAULT_COLUMNS}
    record["source_pdf"] = source_pdf
    record["source_page"] = str(source_page)
    record["floor_label"] = floor_label
    record["trade_category"] = trade_category

    def get_cell(col_name: str) -> str:
        idx = header_map.get(col_name, -1)
        if idx < 0 or idx >= len(row):
            return ""
        return normalize_text(row[idx])

    record["work_name"] = get_cell("work_name")
    record["spec"] = get_cell("spec")
    record["unit"] = get_cell("unit")
    record["qty"] = get_cell("qty")
    record["remark"] = get_cell("remark")
    return record


def extract_plan_rows_from_pdf(pdf_path: Path) -> tuple[list[dict[str, str]], list[ExtractLog]]:
    rows: list[dict[str, str]] = []
    logs: list[ExtractLog] = []

    import pdfplumber  # type: ignore

    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            table_title = find_table_title(page_text)
            if not table_title:
                continue

            floor_label = detect_floor(pdf_path.stem, table_title, page_text)
            trade_category = detect_trade_category(pdf_path.stem, table_title)
            found_any = False

            for table in iter_candidate_tables(page):
                header_idx, header_map = find_header_map(table)
                if header_idx < 0:
                    continue

                data_rows = table[header_idx + 1 :]
                carry_work_name = ""
                table_row_count = 0
                for data_row in data_rows:
                    normalized_row = [normalize_text(cell) for cell in data_row]
                    if not any(normalized_row):
                        continue
                    record = row_to_record(
                        row=normalized_row,
                        header_map=header_map,
                        source_pdf=pdf_path.name,
                        source_page=page_index,
                        floor_label=floor_label,
                        trade_category=trade_category,
                    )

                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    if not record["work_name"] and not record["qty"]:
                        continue
                    rows.append(record)
                    table_row_count += 1

                found_any = found_any or table_row_count > 0
                if table_row_count > 0:
                    logs.append(
                        ExtractLog(
                            source_pdf=pdf_path.name,
                            source_page=page_index,
                            table_title=table_title,
                            row_count=table_row_count,
                            fail_reason="",
                        )
                    )

            if not found_any:
                logs.append(
                    ExtractLog(
                        source_pdf=pdf_path.name,
                        source_page=page_index,
                        table_title=table_title,
                        row_count=0,
                        fail_reason="수량표 키워드 페이지에서 유효 헤더/행 미탐지",
                    )
                )
    return rows, logs


def collect_plan_tables(pdf_dir: Path) -> tuple[list[dict[str, str]], list[ExtractLog]]:
    pdfs = sorted(p for p in pdf_dir.glob("*.pdf") if p.is_file())
    plan_pdfs = [p for p in pdfs if not is_master_pdf(p)]

    all_rows: list[dict[str, str]] = []
    all_logs: list[ExtractLog] = []
    for pdf_path in plan_pdfs:
        try:
            rows, logs = extract_plan_rows_from_pdf(pdf_path)
            all_rows.extend(rows)
            all_logs.extend(logs)
        except Exception as exc:  # noqa: BLE001
            all_logs.append(
                ExtractLog(
                    source_pdf=pdf_path.name,
                    source_page=0,
                    table_title="",
                    row_count=0,
                    fail_reason=f"PDF 처리 실패: {exc}",
                )
            )
    return all_rows, all_logs


def write_plan_csv(rows: list[dict[str, str]], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DEFAULT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="계획도 수량표 추출")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--out_csv", type=Path, default=Path("output/plan_extract.csv"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows, _logs = collect_plan_tables(args.pdf_dir)
    write_plan_csv(rows, args.out_csv)
    print(f"Plan rows: {len(rows)}")


if __name__ == "__main__":
    main()
