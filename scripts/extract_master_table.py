#!/usr/bin/env python3
"""Extract master quantity total table (총괄수량표) from landscape PDFs.

- Master PDF는 파일명에 '총괄수량표'가 없어도 될 수 있음
- 총괄수량표가 '식재(수목)'만 있는 형태(수목명/인정수량/실제수량/지상층/옥상층/비고)도 지원
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable

MASTER_COLUMNS = [
    "work_name",         # 공종/품명/수목명 등 (키)
    "spec",              # 규격
    "unit",              # 단위
    "master_total_qty",  # 총괄의 '실제수량(합계)'로 취급
    "remark",            # 비고
    # 아래는 식재 총괄에서 유용(있으면 채움)
    "recognized_qty",    # 인정수량
    "ground_qty",        # 지상층
    "roof_qty",          # 옥상층(또는 상부층)
    "category",          # 구분
    "symbol",            # 기호
]

# 1) 파일명으로 "마스터 후보"를 고를 때 쓰는 패턴 (완화)
MASTER_FILE_HINT = re.compile(r"(총괄|집계|전체)\s*수량표|수량\s*총괄|summary|master", re.IGNORECASE)

# 2) "마스터 표"를 헤더로 판별할 때 쓰는 핵심 헤더 키워드
#    - 식재 총괄은 '수목명' + '실제수량' 조합이 매우 강함
MASTER_HEADER_HINTS = [
    re.compile(r"실제\s*수량"),
    re.compile(r"인정\s*수량"),
    re.compile(r"총\s*괄|총\s*수량|합\s*계"),
    re.compile(r"수목\s*명|수목명"),
]

COLUMN_CANDIDATES = {
    # 키 컬럼(항목명): 식재형은 '수목명'도 허용
    "work_name": ["공종", "품명", "명칭", "항목", "수목명", "수목 명", "수목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    # 총괄 합계(실제수량): 식재형은 '실제수량'이 핵심
    "master_total_qty": ["합계", "실제수량", "실제 수량", "총수량", "총 수량", "수량", "물량"],
    # 비고
    "remark": ["비고", "참고"],
    # 식재형 추가
    "recognized_qty": ["인정수량", "인정 수량"],
    "ground_qty": ["지상층", "지상", "1층", "2층", "3층"],  # 일반화(실제 매핑은 헤더명 매칭으로)
    "roof_qty": ["옥상층", "옥상", "상부", "R층", "R"],
    "category": ["구분"],
    "symbol": ["기호"],
}


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\r", "\n")).strip()


def is_pdf(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".pdf"


def iter_candidate_tables(page) -> Iterable[list[list[str]]]:
    """여러 설정으로 테이블 후보를 뽑아 중복 제거 후 yield."""
    settings = [
        {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
        {"vertical_strategy": "text", "horizontal_strategy": "text"},
        {"vertical_strategy": "lines", "horizontal_strategy": "text", "intersection_tolerance": 5},
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


def header_row_score(row: list[str]) -> int:
    """헤더 행으로 보일수록 점수↑ (총괄/식재 총괄 헤더를 잡기 위한 휴리스틱)."""
    text = " ".join(normalize_text(c) for c in row if c)
    score = 0
    for pat in MASTER_HEADER_HINTS:
        if pat.search(text):
            score += 2
    # "수목명" + "실제수량" 같이 같이 있으면 추가 가점
    if re.search(r"수목\s*명|수목명", text) and re.search(r"실제\s*수량|실제수량", text):
        score += 3
    return score


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int]]:
    """테이블 상단 몇 줄에서 헤더 후보를 찾아 컬럼 인덱스 매핑 반환."""
    best_idx = -1
    best_map: dict[str, int] = {}
    best_score = -1

    for idx, row in enumerate(table[:8]):
        mapping: dict[str, int] = {}
        for c_idx, cell in enumerate(row):
            cell_norm = normalize_text(cell)
            for key, options in COLUMN_CANDIDATES.items():
                if key in mapping:
                    continue
                if any(opt in cell_norm for opt in options):
                    mapping[key] = c_idx

        score = header_row_score(row)
        # 최소 요건: 이름(공종/수목명 등) + 실제수량(또는 합계)
        if ("work_name" in mapping) and ("master_total_qty" in mapping):
            if score > best_score:
                best_score = score
                best_idx = idx
                best_map = mapping

    return best_idx, best_map


def choose_master_pdf(pdf_dir: Path) -> Path | None:
    """1) 파일명 힌트로 우선 후보를 잡고 2) 없으면 pdf 중 첫 번째를 fallback."""
    pdfs = sorted(p for p in pdf_dir.glob("*.pdf") if is_pdf(p))
    hinted = [p for p in pdfs if MASTER_FILE_HINT.search(p.name)]
    if hinted:
        return hinted[0]
    # fallback: 그냥 첫 pdf(사용자가 폴더에 master만 넣는 경우도 많음)
    return pdfs[0] if pdfs else None


def extract_master_rows(pdf_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    import pdfplumber  # type: ignore

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # ✅ 기존처럼 "총괄" 텍스트로 페이지를 걸러버리면 식재 총괄에서 놓칠 수 있어 제거/완화
            page_text = (page.extract_text() or "").strip()

            # 페이지 자체가 완전 무관하면 스킵(완화된 조건)
            # - 표 힌트가 전혀 없는 페이지는 스킵
            if page_text:
                if not (re.search(r"수량\s*표|수량표|실제\s*수량|실제수량|인정\s*수량|인정수량", page_text)):
                    # 텍스트가 있지만 힌트가 없으면 넘어감
                    continue

            for table in iter_candidate_tables(page):
                header_idx, header_map = find_header_map(table)
                if header_idx < 0:
                    continue

                carry_work_name = ""
                data_rows = table[header_idx + 1 :]

                for r in data_rows:
                    normalized = [normalize_text(cell) for cell in r]
                    if not any(normalized):
                        continue

                    record = {col: "" for col in MASTER_COLUMNS}
                    for col in MASTER_COLUMNS:
                        idx = header_map.get(col, -1)
                        if 0 <= idx < len(normalized):
                            record[col] = normalized[idx]

                    # 공종/수목명 빈칸 carry
                    if not record["work_name"] and carry_work_name:
                        record["work_name"] = carry_work_name
                    if record["work_name"]:
                        carry_work_name = record["work_name"]

                    # 핵심이 둘 다 비면 스킵
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
    parser = argparse.ArgumentParser(description="총괄수량표 추출 (식재형/일반형 지원)")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--out_csv", type=Path, default=Path("output/master_extract.csv"))
    parser.add_argument("--pdf", type=Path, default=None, help="특정 PDF만 지정(선택)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target = args.pdf if args.pdf else choose_master_pdf(args.pdf_dir)
    rows: list[dict[str, str]] = []
    if target and target.exists():
        rows = extract_master_rows(target)
        print(f"Master pdf: {target.name}")
    else:
        print("Master pdf: (not found)")
    write_master_csv(rows, args.out_csv)
    print(f"Master rows: {len(rows)}")


if __name__ == "__main__":
    main()
