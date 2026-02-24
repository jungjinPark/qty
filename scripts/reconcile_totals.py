#!/usr/bin/env python3
"""
Stage-2 (A) presence audit:
- 계획도 수량표의 (항목/규격/단위)가 총괄수량표에 "존재"하는지 여부만 검증
- 합계/차이 계산하지 않음
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path

from extract_master_table import choose_master_pdf, extract_master_rows, write_master_csv
from extract_plan_tables import ExtractLog, collect_plan_tables, write_plan_csv


# === Output columns (A: 존재 검증용) ===
SUMMARY_COLUMNS = [
    "source_pdf",
    "source_page",
    "floor_label",
    "trade_category",
    "work_name",
    "spec",
    "unit",
    "qty",          # 참고용(계산 안 함)
    "status",       # FOUND / NOT_FOUND / EXCLUDED / WEAK_FOUND
    "match_level",  # EXACT / NO_UNIT / NO_SPEC / NAME_ONLY / NONE
    "master_hit",   # 매칭된 총괄측 키(참고)
    "master_pages", # (있으면) 총괄측 페이지 정보(현재 master 추출에 page가 없으면 빈칸)
]

# === Exclude rules ===
# 표 헤더/설명/소계/합계/계 같은 “검수 대상이 아닌 행”을 걸러서 false negative를 줄임
EXCLUDE_EXACT = {
    "구분", "기호", "수목명", "품명", "명칭", "항목", "규격", "단위",
    "인정수량", "실제수량", "지상층", "옥상층", "비고",
    "합계", "총계", "소계",
    "교목계", "관목계", "초화류계", "식재지반계",
    "교목 계", "관목 계", "초화류 계", "식재지반 계",
}

EXCLUDE_CONTAINS = [
    "NOTE", "노트", "비고", "주)", "주요", "기준", "설명",
]

# “~계”가 항목명이 아니라 그룹/소계 성격인 경우가 많아 제외
EXCLUDE_SUFFIX_PATTERNS = [
    re.compile(r".*계$"),      # 교목계/관목계/포장계...
    re.compile(r".*소계$"),
    re.compile(r".*합계$"),
]


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", "\n")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_key_text(value: str | None) -> str:
    return normalize_text(value)


def is_excluded_work_name(work_name: str) -> bool:
    wn = clean_key_text(work_name)
    if not wn:
        return True

    # exact match (공백 제거한 버전도 같이 확인)
    wn_nospace = wn.replace(" ", "")
    if wn in EXCLUDE_EXACT or wn_nospace in {x.replace(" ", "") for x in EXCLUDE_EXACT}:
        return True

    # contains keywords
    up = wn.upper()
    for k in EXCLUDE_CONTAINS:
        if k.upper() in up:
            return True

    # suffix patterns
    for pat in EXCLUDE_SUFFIX_PATTERNS:
        if pat.match(wn_nospace):
            return True

    return False


def write_extract_log(logs: list[ExtractLog], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("file,page,table_title,row_count,fail_reason\n")
        for log in logs:
            f.write(
                f'"{log.source_pdf}",{log.source_page},"{log.table_title}",{log.row_count},"{log.fail_reason}"\n'
            )


def write_csv(rows: list[dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_excel(
    out_path: Path,
    audit_rows: list[dict[str, str]],
    plan_rows: list[dict[str, str]],
    master_rows: list[dict[str, str]],
) -> bool:
    """
    pandas/openpyxl이 있으면 엑셀 생성, 없으면 False
    """
    try:
        import pandas as pd  # type: ignore
    except Exception:
        return False

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 빈 리스트여도 컬럼이 살아있게 보장
    audit_df = pd.DataFrame(audit_rows, columns=SUMMARY_COLUMNS)

    status_counts = Counter((r.get("status") or "") for r in audit_rows)
    summary_table = pd.DataFrame(
        [{"metric": "total_plan_items", "value": len(plan_rows)}]
        + [{"metric": "total_master_items", "value": len(master_rows)}]
        + [{"metric": "total_audit_rows", "value": len(audit_rows)}]
        + [{"metric": f"status_{k}", "value": v} for k, v in sorted(status_counts.items())]
    )

    def df_filter(status: str) -> "pd.DataFrame":
        if "status" not in audit_df.columns or len(audit_df) == 0:
            return pd.DataFrame(columns=SUMMARY_COLUMNS)
        return audit_df[audit_df["status"] == status]

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_table.to_excel(writer, sheet_name="Summary", index=False)
        df_filter("NOT_FOUND").to_excel(writer, sheet_name="NotFound", index=False)
        df_filter("WEAK_FOUND").to_excel(writer, sheet_name="WeakFound", index=False)
        df_filter("EXCLUDED").to_excel(writer, sheet_name="Excluded", index=False)
        df_filter("FOUND").to_excel(writer, sheet_name="Found", index=False)
        pd.DataFrame(plan_rows).to_excel(writer, sheet_name="RawPlanExtract", index=False)
        pd.DataFrame(master_rows).to_excel(writer, sheet_name="RawMasterExtract", index=False)

    return True


def audit_presence(
    plan_rows: list[dict[str, str]],
    master_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """
    plan_rows 각각에 대해 총괄(master_rows)에 존재하는지 체크.
    - 1차: (work_name, spec, unit) EXACT
    - 2차: (work_name, spec)     NO_UNIT
    - 3차: (work_name, unit)     NO_SPEC
    - 4차: (work_name)           NAME_ONLY
    """
    master_exact: set[tuple[str, str, str]] = set()
    master_name_spec: set[tuple[str, str]] = set()
    master_name_unit: set[tuple[str, str]] = set()
    master_name_only: set[str] = set()

    # master 인덱스 구축
    for m in master_rows:
        wn = clean_key_text(m.get("work_name"))
        sp = clean_key_text(m.get("spec"))
        un = clean_key_text(m.get("unit"))

        if not wn or is_excluded_work_name(wn):
            continue

        master_exact.add((wn, sp, un))
        master_name_spec.add((wn, sp))
        master_name_unit.add((wn, un))
        master_name_only.add(wn)

    results: list[dict[str, str]] = []

    for p in plan_rows:
        wn = clean_key_text(p.get("work_name"))
        sp = clean_key_text(p.get("spec"))
        un = clean_key_text(p.get("unit"))
        qty = clean_key_text(p.get("qty"))

        if not wn or is_excluded_work_name(wn):
            status = "EXCLUDED"
            match_level = "NONE"
            master_hit = ""
        else:
            if (wn, sp, un) in master_exact:
                status = "FOUND"
                match_level = "EXACT"
                master_hit = f"{wn} | {sp} | {un}"
            elif (wn, sp) in master_name_spec:
                status = "WEAK_FOUND"
                match_level = "NO_UNIT"
                master_hit = f"{wn} | {sp}"
            elif (wn, un) in master_name_unit:
                status = "WEAK_FOUND"
                match_level = "NO_SPEC"
                master_hit = f"{wn} | {un}"
            elif wn in master_name_only:
                status = "WEAK_FOUND"
                match_level = "NAME_ONLY"
                master_hit = f"{wn}"
            else:
                status = "NOT_FOUND"
                match_level = "NONE"
                master_hit = ""

        results.append(
            {
                "source_pdf": p.get("source_pdf", ""),
                "source_page": str(p.get("source_page", "")),
                "floor_label": p.get("floor_label", ""),
                "trade_category": p.get("trade_category", ""),
                "work_name": wn,
                "spec": sp,
                "unit": un,
                "qty": qty,
                "status": status,
                "match_level": match_level,
                "master_hit": master_hit,
                "master_pages": "",  # master 추출에 page가 없으면 빈칸 유지
            }
        )

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="(A) 항목 존재 여부 검수 파이프라인")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--outdir", type=Path, default=Path("output"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    # 1) 계획도 수량표 추출
    plan_rows, plan_logs = collect_plan_tables(args.pdf_dir)

    # 2) 총괄수량표 선택/추출 (L-003 고정 아님: choose_master_pdf가 패턴으로 찾음)
    master_pdf = choose_master_pdf(args.pdf_dir)
    master_rows = extract_master_rows(master_pdf) if master_pdf else []

    # 3) (A) 존재 검증
    audit_rows = audit_presence(plan_rows, master_rows)

    # 4) 산출물 저장
    write_csv(audit_rows, args.outdir / "recon_summary.csv")
    excel_ok = write_excel(args.outdir / "recon_detail.xlsx", audit_rows, plan_rows, master_rows)

    # 추출 원본도 같이 저장(디버깅/검수용)
    write_plan_csv(plan_rows, args.outdir / "plan_extract.csv")
    write_master_csv(master_rows, args.outdir / "master_extract.csv")

    # plan 추출 로그
    write_extract_log(plan_logs, args.outdir / "extract_log.txt")

    if not excel_ok:
        with (args.outdir / "extract_log.txt").open("a", encoding="utf-8") as f:
            f.write('"system",0,"",0,"recon_detail.xlsx 생성 스킵: pandas/openpyxl 미설치"\n')

    print(f"Plan rows: {len(plan_rows)}")
    print(f"Master rows: {len(master_rows)}")
    print(f"Audit rows: {len(audit_rows)}")
    if master_pdf:
        print(f"Master pdf: {master_pdf.name}")
    else:
        print("Master pdf: NOT FOUND")


if __name__ == "__main__":
    main()
