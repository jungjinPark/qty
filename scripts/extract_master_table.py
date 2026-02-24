#!/usr/bin/env python3
"""Extract master quantity total table (총괄수량표) from landscape PDFs.

- L-003 고정 의존 제거: 파일명/텍스트 스캔으로 총괄수량표 PDF를 추정
- '총 괄 수 량 표' 처럼 공백/줄바꿈이 섞여도 탐지
- 페이지 텍스트 필터 완화(텍스트가 비어도 표 추출 시도)
- 헤더 매칭도 공백 제거한 문자열로 비교
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, Optional

MASTER_COLUMNS = ["work_name", "spec", "unit", "master_total_qty", "remark"]

# 파일명에서 총괄수량표 후보를 찾을 때(도면번호가 L-003이 아닐 수도 있으므로 텍스트 중심)
FILENAME_MASTER_HINT = re.compile(r"(총괄\s*수량\s*표|총괄\s*물량\s*표|수량\s*총괄)", re.IGNORECASE)

# PDF 텍스트에서 총괄수량표인지 판단(공백/개행 무시용)
TEXT_MASTER_HINTS = [
    "총괄수량표",
    "총괄물량표",
    "수량총괄",
]

# 헤더 후보(공백/개행 제거한 문자열 기준으로 매칭)
COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    "master_total_qty": ["합계", "실제수량", "총수량", "수량", "물량", "합", "계"],
    "remark": ["비고", "참고"],
}


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    # pdfplumber 결과는 None/개행/복수공백이 흔함
    return re.sub(r"\s+", " ", str(value).replace("\r", "\n")).strip()


def compact_text(value: str | None) -> str:
    """공백/개행 제거한 비교용 문자열"""
    return re.sub(r"\s+", "", normalize_text(value))


def iter_candidate_tables(page) -> Iterable[list[list[str]]]:
    """여러 table_settings로 표 추출을 시도"""
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
            cleaned = tuple(tuple(normalize_text(cell) for cell in row) for row in table if row)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            yield [list(row) for row in cleaned]


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int]]:
    """상단 몇 줄에서 헤더를 찾고 컬럼 인덱스 매핑 생성"""
    for r_idx, row in enumerate(table[:8]):
        mapping: dict[str, int] = {}
        for c_idx, cell in enumerate(row):
            cell_compact = compact_text(cell)
            if not cell_compact:
                continue

            for key, options in COLUMN_CANDIDATES.items():
                if key in mapping:
                    continue
                # 옵션도 compact 비교
                for opt in options:
                    if opt and opt in cell_compact:
                        mapping[key] = c_idx
                        break

        # 최소 조건: 공종/항목 + 합계/수량 계열이 있어야 함
        if "work_name" in mapping and "master_total_qty" in mapping:
            return r_idx, mapping

    return -1, {}


def page_looks_like_master(page) -> bool:
    """페이지 텍스트로 총괄수량표 여부를 대충 판단(공백 제거 비교)"""
    text = page.extract_text() or ""
    t = re.sub(r"\s+", "", text)
    return any(h in t for h in TEXT_MASTER_HINTS)


def choose_master_pdf(pdf_dir: Path) -> Optional[Path]:
    """총괄수량표 PDF를 선택
    1) 파일명 힌트(총괄수량표/수량총괄 등) 우선
    2) 텍스트 스캔으로 '총괄수량표'가 있는 PDF 우선
    3) 그래도 없으면 None
    """
    pdfs = sorted(p for p in pdf_dir.glob("*.pdf") if p.is_file())

    # 1) 파일명 기반
    name_hits = [p for p in pdfs if FILENAME_MASTER_HINT.search(p.name)]
    if name_hits:
        return name_hits[0]

    # 2) 텍스트 스캔(첫 2~3페이지 정도만 확인)
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return None

    best: Optional[Path] = None
    best_score = -1
    for p in pdfs:
        score = 0
        try:
            with pdfplumber.open(p) as pdf:
                for page in pdf.pages[:3]:
                    if page_looks_like_master(page):
                        score += 10
        except Exception:
            continue

        if score > best_score:
            best_score = score
            best = p

    # score가 0이면 “총괄수량표 느낌”이 전혀 없는 것 → None 처리(오탐 방지)
    if best_score <= 0:
        return None
    return best


def extract_master_rows(pdf_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    import pdfplumber  # type: ignore

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # ✅ 기존처럼 "총괄/수량표" 텍스트 없으면 continue 하지 않음
            #    (텍스트 추출이 빈 페이지라도 표는 추출될 수 있음)

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
                        if 0 <= idx < len(normalized):
                            record[col] = normalized[idx]

                    # 공종(항목) 누락 시 상단 carry
                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    # 공종/합계 둘 다 비면 무시
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
    if not target:
        print("[WARN] 총괄수량표 PDF를 자동으로 찾지 못했습니다. (파일명/텍스트 힌트 없음)")
        write_master_csv([], args.out_csv)
        return

    rows = extract_master_rows(target)
    write_master_csv(rows, args.out_csv)
    print(f"Master pdf: {target.name}")
    print(f"Master rows: {len(rows)}")


if __name__ == "__main__":
    main()
