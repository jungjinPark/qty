#!/usr/bin/env python3
"""Extract plan quantity tables (수량표) from landscape PDFs.

핵심 개선점
- '수량표' 텍스트가 페이지에서 안 잡혀도(이미지/도면선/폰트) 표 헤더로 탐지 시도
- '수량표' 외에도 수량산출표/수량집계표 등 다양한 제목 허용(있으면 기록)
- 헤더가 2줄로 나뉘는 경우(예: "실제" + "수량") 병합해서 탐지
- table_settings 다양화 + 간단 스코어링으로 유효 표 우선
- 총괄수량표(L-003 고정 아님) 제외 로직 완화: 파일명에 '총괄수량표' 있으면 제외
"""

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

# '수량표'가 텍스트로 안 잡히는 경우가 많아서 "있으면 참고"로만 쓰고,
# 실제 추출 여부는 헤더 탐지로 결정한다.
TABLE_TITLE_PATTERN = re.compile(r"(수량\s*표|수량\s*산출\s*표|수량\s*집계\s*표|수량\s*내역\s*표)", re.IGNORECASE)

# 총괄수량표 파일 제외: L-003 고정은 버리고 '총괄수량표' 키워드 중심
SUMMARY_FILE_PATTERN = re.compile(r"(총괄\s*수량\s*표|총괄수량표)", re.IGNORECASE)

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

# 헤더 후보 (수량은 매우 다양하므로 조금 넓게)
COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    "qty": ["수량", "물량", "량", "합계", "총수량", "실제수량"],
    "remark": ["비고", "참고"],
}

SKIP_WORK_TOKENS = ("소계", "합계", "계", "subtotal", "total")


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
    # 총괄수량표만 확실히 제외. (도면번호가 바뀌어도 문제 없게)
    return bool(SUMMARY_FILE_PATTERN.search(path.name))


def _merge_two_rows(row1: list[str], row2: list[str]) -> list[str]:
    n = max(len(row1), len(row2))
    out: list[str] = []
    for i in range(n):
        a = row1[i] if i < len(row1) else ""
        b = row2[i] if i < len(row2) else ""
        out.append(normalize_text(f"{a} {b}"))
    return out


def iter_candidate_tables(page) -> Iterable[list[list[str]]]:
    # 표 인식이 도면마다 달라서 settings 다양화
    settings = [
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {"vertical_strategy": "lines", "horizontal_strategy": "text", "intersection_tolerance": 5},
        {"vertical_strategy": "text", "horizontal_strategy": "lines", "intersection_tolerance": 5},
        {"vertical_strategy": "lines", "horizontal_strategy": "lines", "snap_tolerance": 3},
    ]

    seen: set[tuple[tuple[str, ...], ...]] = set()
    for setting in settings:
        tables = page.extract_tables(table_settings=setting) or []
        for table in tables:
            cleaned_rows: list[list[str]] = []
            for row in table or []:
                if not row:
                    continue
                cleaned_rows.append([normalize_text(cell) for cell in row])

            key = tuple(tuple(r) for r in cleaned_rows if any(r))
            if not key or key in seen:
                continue
            seen.add(key)
            yield cleaned_rows


def find_table_title(page_text: str) -> str:
    # 있으면 로그용으로만 활용
    if not page_text:
        return ""
    for line in page_text.splitlines():
        line = normalize_text(line)
        if TABLE_TITLE_PATTERN.search(line):
            return line
    return ""


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int], int]:
    """
    return: (header_idx, header_map, score)
    - score가 높을수록 '수량표 헤더'에 가까움
    """
    max_scan = min(len(table), 10)
    best_idx = -1
    best_map: dict[str, int] = {}
    best_score = 0

    for i in range(max_scan):
        row = [normalize_text(c) for c in table[i]]

        tries = [(row, i)]
        if i + 1 < max_scan:
            row2 = [normalize_text(c) for c in table[i + 1]]
            tries.append((_merge_two_rows(row, row2), i))

        for header_like, header_idx in tries:
            mapping: dict[str, int] = {}
            for c_idx, cell in enumerate(header_like):
                if not cell:
                    continue
                for target, options in COLUMN_CANDIDATES.items():
                    if target in mapping:
                        continue
                    if any(option in cell for option in options):
                        mapping[target] = c_idx

            # 스코어링: 핵심은 work_name + qty
            score = 0
            if "work_name" in mapping:
                score += 3
            if "qty" in mapping:
                score += 3
            if "unit" in mapping:
                score += 1
            if "spec" in mapping:
                score += 1
            if "remark" in mapping:
                score += 1

            # 최소조건: work_name + qty가 있어야 "수량표"로 본다
            if score > best_score and ("work_name" in mapping and "qty" in mapping):
                best_score = score
                best_idx = header_idx
                best_map = mapping

    return best_idx, best_map, best_score


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

            # floor/trade는 텍스트가 없어도 파일명 기반으로라도 잡아둔다
            floor_label = detect_floor(pdf_path.stem, table_title, page_text)
            trade_category = detect_trade_category(pdf_path.stem, table_title, page_text)

            found_any_table = False
            extracted_rows_this_page = 0
            fail_reason = ""

            # ★ 변경: "수량표" 제목 유무와 상관없이 표를 훑는다.
            for table in iter_candidate_tables(page):
                header_idx, header_map, score = find_header_map(table)
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

                    # 공종명 carry
                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    wn = record["work_name"].strip()
                    if wn and any(tok.lower() in wn.lower() for tok in SKIP_WORK_TOKENS):
                        continue

                    # work_name/qty 둘다 비면 skip
                    if not record["work_name"] and not record["qty"]:
                        continue

                    rows.append(record)
                    table_row_count += 1

                if table_row_count > 0:
                    found_any_table = True
                    extracted_rows_this_page += table_row_count
                    logs.append(
                        ExtractLog(
                            source_pdf=pdf_path.name,
                            source_page=page_index,
                            table_title=table_title,
                            row_count=table_row_count,
                            fail_reason="",
                        )
                    )

            # 페이지에서 아무것도 못 뽑았을 때 로그
            if not found_any_table:
                # 텍스트는 있는데 헤더가 안 잡히는 경우와, 텍스트 자체가 빈 경우를 구분
                if page_text.strip():
                    fail_reason = "표 후보 탐지/헤더 미탐지 (페이지 텍스트는 존재)"
                else:
                    fail_reason = "페이지 텍스트 추출 실패(이미지 가능) + 표 헤더 미탐지"
                logs.append(
                    ExtractLog(
                        source_pdf=pdf_path.name,
                        source_page=page_index,
                        table_title=table_title,
                        row_count=0,
                        fail_reason=fail_reason,
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


def write_extract_log(logs: list[ExtractLog], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("file,page,table_title,row_count,fail_reason\n")
        for log in logs:
            f.write(
                f'"{log.source_pdf}",{log.source_page},"{log.table_title}",{log.row_count},"{log.fail_reason}"\n'
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="계획도 수량표 추출")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--out_csv", type=Path, default=Path("output/plan_extract.csv"))
    parser.add_argument("--out_log", type=Path, default=Path("output/plan_extract_log.txt"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows, logs = collect_plan_tables(args.pdf_dir)
    write_plan_csv(rows, args.out_csv)
    write_extract_log(logs, args.out_log)
    print(f"Plan rows: {len(rows)}")
    print(f"Log: {args.out_log}")


if __name__ == "__main__":
    main()
