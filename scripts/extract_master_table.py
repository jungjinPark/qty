#!/usr/bin/env python3
"""Extract master quantity total table (총괄수량표) from PDFs.

- 총괄수량표 도면번호가 L-003이 아닐 수도 있으므로 파일명/본문/헤더스코어로 자동 탐지
- (공종/규격/단위/합계(실제수량)/비고) 추출
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

MASTER_COLUMNS = ["work_name", "spec", "unit", "master_total_qty", "remark"]

# 파일명/본문에서 총괄수량표를 찾기 위한 키워드(여유있게)
MASTER_NAME_HINT_RE = re.compile(r"(총괄\s*수량\s*표|총괄수량표|총괄\s*수량|MASTER\s*QTY)", re.IGNORECASE)
MASTER_TEXT_HINT_RE = re.compile(r"(총괄\s*수량\s*표|총괄수량표|총괄\s*수량)", re.IGNORECASE)

COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    # master_total_qty: 합계/실제수량 계열 우선
    "master_total_qty": ["합계", "실제수량", "총수량", "총합계", "물량", "수량"],
    "remark": ["비고", "참고"],
}

MASTER_QTY_PRIORITY = ["합계", "실제수량", "총합계", "총수량", "물량", "수량"]

SKIP_WORK_TOKENS = ("소계", "합계", "계", "subtotal", "total")


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    txt = str(value).replace("\r", "\n")
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def _match_any(cell: str, options: list[str]) -> bool:
    return any(opt in cell for opt in options)


def _merge_two_rows(row1: list[str], row2: list[str]) -> list[str]:
    n = max(len(row1), len(row2))
    out: list[str] = []
    for i in range(n):
        a = row1[i] if i < len(row1) else ""
        b = row2[i] if i < len(row2) else ""
        out.append(normalize_text(f"{a} {b}"))
    return out


def _choose_qty_col_index(header_cells: list[str]) -> Optional[int]:
    candidates: list[tuple[int, str]] = []
    for idx, cell in enumerate(header_cells):
        if not cell:
            continue
        if any(k in cell for k in COLUMN_CANDIDATES["master_total_qty"]):
            candidates.append((idx, cell))
    if not candidates:
        return None

    def rank(cell_text: str) -> int:
        for r, key in enumerate(MASTER_QTY_PRIORITY):
            if key in cell_text:
                return r
        return 999

    candidates.sort(key=lambda x: rank(x[1]))
    return candidates[0][0]


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int], int]:
    """
    return: (header_idx, header_map, score)
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
                for key, opts in COLUMN_CANDIDATES.items():
                    if key in mapping:
                        continue
                    if _match_any(cell, opts):
                        mapping[key] = c_idx

            # 핵심 컬럼 가중치
            score = 0
            if "work_name" in mapping:
                score += 3
            if "master_total_qty" in mapping:
                score += 3
            if "unit" in mapping:
                score += 1
            if "spec" in mapping:
                score += 1
            if "remark" in mapping:
                score += 1

            if score > best_score and ("work_name" in mapping and "master_total_qty" in mapping):
                best_score = score
                best_idx = header_idx
                best_map = mapping

    if best_idx >= 0 and best_map:
        header_row = [normalize_text(c) for c in table[best_idx]]
        header_row2 = [normalize_text(c) for c in table[best_idx + 1]] if best_idx + 1 < len(table) else []
        merged = _merge_two_rows(header_row, header_row2) if header_row2 else header_row
        qty_col = _choose_qty_col_index(merged)
        if qty_col is not None:
            best_map["master_total_qty"] = qty_col

    return best_idx, best_map, best_score


def iter_candidate_tables(page) -> Iterable[list[list[str]]]:
    settings = [
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {"vertical_strategy": "lines", "horizontal_strategy": "text", "intersection_tolerance": 5},
        {"vertical_strategy": "text", "horizontal_strategy": "lines", "intersection_tolerance": 5},
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

            cleaned_key = tuple(tuple(r) for r in cleaned_rows if any(r))
            if not cleaned_key or cleaned_key in seen:
                continue
            seen.add(cleaned_key)
            yield cleaned_rows


def extract_master_rows(pdf_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not pdf_path.exists():
        return rows

    import pdfplumber  # type: ignore

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in iter_candidate_tables(page):
                header_idx, header_map, score = find_header_map(table)
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
                        if 0 <= idx < len(normalized):
                            record[col] = normalized[idx]

                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    wn = record["work_name"].strip()
                    if wn and any(tok.lower() in wn.lower() for tok in SKIP_WORK_TOKENS):
                        continue

                    if not record["work_name"] and not record["master_total_qty"]:
                        continue

                    rows.append(record)

    return rows


@dataclass
class MasterPick:
    path: Path
    name_hint: bool
    text_hint: bool
    header_score: int
    rows_count: int


def _has_text_hint(pdf_path: Path, pages_to_check: int = 2) -> bool:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return False

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages[:pages_to_check]):
                txt = page.extract_text() or ""
                if MASTER_TEXT_HINT_RE.search(txt):
                    return True
    except Exception:
        return False
    return False


def _estimate_header_score(pdf_path: Path, pages_to_check: int = 2) -> int:
    """총괄표 '같은' 헤더가 얼마나 잘 잡히는지 대충 스코어링."""
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return 0

    best = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:pages_to_check]:
                for table in iter_candidate_tables(page):
                    _, _, score = find_header_map(table)
                    best = max(best, score)
    except Exception:
        return best
    return best


def choose_master_pdf(pdf_dir: Path) -> Path | None:
    """L-003 고정이 아니라도 총괄수량표를 자동 선택."""
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        return None

    picks: list[MasterPick] = []
    for p in pdfs:
        name_hint = bool(MASTER_NAME_HINT_RE.search(p.name))
        text_hint = _has_text_hint(p, pages_to_check=2) if not name_hint else True
        header_score = _estimate_header_score(p, pages_to_check=2)
        # rows_count는 비용이 크므로(전페이지 추출) 여기서는 스킵.
        picks.append(MasterPick(p, name_hint, text_hint, header_score, rows_count=0))

    # 정렬 기준:
    # 1) 파일명 힌트
    # 2) 본문 힌트
    # 3) 헤더 스코어
    picks.sort(key=lambda x: (x.name_hint, x.text_hint, x.header_score), reverse=True)
    best = picks[0]

    # 최소한 헤더스코어가 의미있어야 선택 (0이면 실패 가능성 큼)
    if not best.name_hint and not best.text_hint and best.header_score == 0:
        return None

    return best.path


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
    print(f"Master pdf: {target.name if target else 'NOT_FOUND'}")
    print(f"Master rows: {len(rows)}")


if __name__ == "__main__":
    main()
