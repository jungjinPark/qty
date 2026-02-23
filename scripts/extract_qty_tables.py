#!/usr/bin/env python3
"""Extract quantity tables (수량표) from landscape drawing PDFs into CSV files."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

OUTPUT_COLUMNS = [
    "파일명",
    "도면번호",
    "층구분",
    "공종",
    "구분",
    "품명",
    "규격",
    "단위",
    "수량",
    "비고",
    "원본페이지",
]

TABLE_TITLE_PATTERN = re.compile(r"[\w\s\-·()\[\]가-힣]*수량표")
SUMMARY_PATTERN = re.compile(r"총\s*괄\s*수량표|총\s*괄")

FLOOR_PATTERNS = [
    re.compile(r"(옥상층)"),
    re.compile(r"(지상\s*\d+\s*층)"),
    re.compile(r"(지하\s*\d+\s*층)"),
    re.compile(r"(B\s*\d+\s*층?)", re.IGNORECASE),
    re.compile(r"(?<!지상)(?<!지하)(\b\d+\s*층\b)"),
    re.compile(r"(지상층)"),
]

DISCIPLINE_RULES = {
    "식재지반": ["식재지반"],
    "식재": ["식재"],
    "시설물": ["시설물"],
    "포장": ["포장"],
    "관수": ["관수"],
    "조명": ["조명", "조명기기"],
    "총괄": ["총괄"],
}

HEADER_MAP = {
    "구분": ["구분", "종별", "세부구분"],
    "품명": ["품명", "명칭", "재료명", "항목"],
    "규격": ["규격", "치수", "사양"],
    "단위": ["단위"],
    "수량": ["수량", "물량", "량"],
    "비고": ["비고", "참고"],
}


@dataclass
class ExtractLog:
    file_name: str
    floor: str
    detected: bool
    pages: list[int]
    rows: int
    reason: str = ""


def normalize_cell(value: str | None) -> str:
    if value is None:
        return ""
    value = str(value).replace("\r", "\n")
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip()


def detect_floor_from_text(text: str) -> str | None:
    for pattern in FLOOR_PATTERNS:
        match = pattern.search(text)
        if match:
            return re.sub(r"\s+", "", match.group(1))
    return None


def detect_floor(file_name: str, page_text: str, is_summary: bool) -> str:
    if is_summary:
        return "총괄"

    floor = detect_floor_from_text(file_name)
    if floor:
        return floor

    floor = detect_floor_from_text(page_text)
    if floor:
        return floor

    return "미지정"


def detect_discipline(file_name: str, table_title: str, is_summary: bool) -> str:
    if is_summary:
        return "총괄"

    source = f"{file_name} {table_title}"
    for discipline, keywords in DISCIPLINE_RULES.items():
        if discipline == "총괄":
            continue
        if any(keyword in source for keyword in keywords):
            return discipline
    return "기타"


def detect_table_title(page_text: str) -> str | None:
    for line in page_text.splitlines():
        line = normalize_cell(line)
        if TABLE_TITLE_PATTERN.search(line):
            return line
    return None


def detect_drawing_no(file_name: str, page_text: str) -> str:
    stem = Path(file_name).stem
    m = re.search(r"([A-Za-z]{1,4}-?\d{1,4}(?:-\d{1,3})?)", stem)
    if m:
        return m.group(1)

    for line in page_text.splitlines():
        line = normalize_cell(line)
        if "도면번호" in line:
            mm = re.search(r"도면번호\s*[:：]?\s*([\w\-./]+)", line)
            if mm:
                return mm.group(1)
    return ""


def _find_header_indices(row: Sequence[str]) -> dict[str, int]:
    indices: dict[str, int] = {}
    for idx, cell in enumerate(row):
        cell_norm = normalize_cell(cell)
        for target, candidates in HEADER_MAP.items():
            if target in indices:
                continue
            if any(candidate in cell_norm for candidate in candidates):
                indices[target] = idx
    return indices


def try_parse_quantity(value: str) -> str:
    raw = normalize_cell(value)
    if not raw:
        return ""
    compact = raw.replace(",", "")
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", compact):
        return compact
    return raw


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

    seen = set()
    for setting in settings:
        tables = page.extract_tables(table_settings=setting) or []
        for table in tables:
            cleaned = tuple(tuple(normalize_cell(c) for c in row) for row in table if row)
            if not cleaned:
                continue
            if cleaned in seen:
                continue
            seen.add(cleaned)
            yield [list(row) for row in cleaned]


def extract_rows_from_table(
    table: list[list[str]],
    file_name: str,
    page_no: int,
    floor: str,
    discipline: str,
    drawing_no: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not table:
        return rows

    header_idx = None
    mapping: dict[str, int] = {}

    for i, row in enumerate(table[:4]):
        found = _find_header_indices(row)
        if "품명" in found and "수량" in found:
            header_idx = i
            mapping = found
            break

    data_rows = table[header_idx + 1 :] if header_idx is not None else table

    for row in data_rows:
        cells = [normalize_cell(c) for c in row]
        if not any(cells):
            continue

        record = {col: "" for col in OUTPUT_COLUMNS}
        record["파일명"] = file_name
        record["도면번호"] = drawing_no
        record["층구분"] = floor
        record["공종"] = discipline
        record["원본페이지"] = str(page_no)

        if mapping:
            for key in ["구분", "품명", "규격", "단위", "수량", "비고"]:
                idx = mapping.get(key)
                record[key] = cells[idx] if idx is not None and idx < len(cells) else ""
        else:
            # Fallback positional parsing while preserving raw text.
            record["구분"] = cells[0] if len(cells) > 0 else ""
            record["품명"] = cells[1] if len(cells) > 1 else ""
            record["규격"] = cells[2] if len(cells) > 2 else ""
            record["단위"] = cells[3] if len(cells) > 3 else ""
            record["수량"] = cells[4] if len(cells) > 4 else ""
            record["비고"] = cells[5] if len(cells) > 5 else ""

        if not record["품명"] and not record["수량"]:
            continue

        record["수량"] = try_parse_quantity(record["수량"])
        rows.append(record)

    return rows


def process_pdf(pdf_path: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], ExtractLog]:
    all_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []

    detected_pages: list[int] = []
    floor_first = "미지정"

    try:
        import pdfplumber  # type: ignore

        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                table_title = detect_table_title(page_text)
                has_qty_keyword = bool(table_title)

                page_tables = list(iter_candidate_tables(page))
                if not page_tables:
                    continue

                for table in page_tables:
                    joined_head = " ".join(" ".join(r) for r in table[:3])
                    inferred_title = table_title or joined_head
                    is_summary = bool(SUMMARY_PATTERN.search(inferred_title))

                    header_found = any(
                        ("품명" in " ".join(r) and "수량" in " ".join(r)) for r in table[:4]
                    )
                    has_qty_context = has_qty_keyword or "수량표" in inferred_title
                    if not (header_found and has_qty_context):
                        continue

                    floor = detect_floor(pdf_path.name, page_text, is_summary)
                    if floor_first == "미지정":
                        floor_first = floor

                    discipline = detect_discipline(pdf_path.name, inferred_title, is_summary)
                    drawing_no = detect_drawing_no(pdf_path.name, page_text)

                    extracted = extract_rows_from_table(
                        table=table,
                        file_name=pdf_path.name,
                        page_no=page_idx,
                        floor=floor,
                        discipline=discipline,
                        drawing_no=drawing_no,
                    )
                    if not extracted:
                        continue

                    detected_pages.append(page_idx)
                    all_rows.extend(extracted)
                    if is_summary:
                        summary_rows.extend(extracted)

    except Exception as exc:  # noqa: BLE001
        log = ExtractLog(
            file_name=pdf_path.name,
            floor="미지정",
            detected=False,
            pages=[],
            rows=0,
            reason=f"PDF 처리 실패: {exc}",
        )
        return [], [], log

    detected = bool(all_rows)
    reason = "" if detected else "수량표 또는 유효 행 미탐지"
    log = ExtractLog(
        file_name=pdf_path.name,
        floor=floor_first,
        detected=detected,
        pages=sorted(set(detected_pages)),
        rows=len(all_rows),
        reason=reason,
    )
    return all_rows, summary_rows, log


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_logs(path: Path, logs: list[ExtractLog]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for log in logs:
            fp.write(f"파일: {log.file_name}\n")
            fp.write(f"- 층 인식 결과: {log.floor}\n")
            fp.write(f"- 수량표 탐지 여부: {'Y' if log.detected else 'N'}\n")
            fp.write(f"- 추출 페이지: {', '.join(map(str, log.pages)) if log.pages else '-'}\n")
            fp.write(f"- 추출 행 수: {log.rows}\n")
            if log.reason:
                fp.write(f"- 실패 사유: {log.reason}\n")
            fp.write("\n")


def collect_pdfs(input_dir: Path) -> list[Path]:
    return sorted(p for p in input_dir.rglob("*.pdf") if p.is_file())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="조경 도면 PDF 수량표 추출기")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("."),
        help="PDF 검색 루트 디렉터리 (기본: 현재 디렉터리)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("extracted"),
        help="출력 디렉터리 (기본: extracted)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pdfs = collect_pdfs(args.input_dir)

    all_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []
    logs: list[ExtractLog] = []

    try:
        import pdfplumber  # type: ignore  # noqa: F401
        pdf_ready = True
    except Exception:  # noqa: BLE001
        pdf_ready = False

    if not pdfs:
        logs.append(
            ExtractLog(
                file_name="-",
                floor="미지정",
                detected=False,
                pages=[],
                rows=0,
                reason="입력 디렉터리에서 PDF를 찾지 못함",
            )
        )

    if pdfs and not pdf_ready:
        for pdf in pdfs:
            logs.append(
                ExtractLog(
                    file_name=pdf.name,
                    floor="미지정",
                    detected=False,
                    pages=[],
                    rows=0,
                    reason="pdfplumber 미설치로 추출 불가",
                )
            )
    else:
        for pdf in pdfs:
            rows, s_rows, log = process_pdf(pdf)
            all_rows.extend(rows)
            summary_rows.extend(s_rows)
            logs.append(log)

    write_csv(args.output_dir / "plan_tables.csv", all_rows)
    write_csv(args.output_dir / "summary_table.csv", summary_rows)
    write_logs(args.output_dir / "extract_log.txt", logs)

    print(f"Processed PDFs: {len(pdfs)}")
    print(f"Extracted rows: {len(all_rows)}")
    print(f"Summary rows: {len(summary_rows)}")


if __name__ == "__main__":
    main()
