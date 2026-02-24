#!/usr/bin/env python3
"""Stage-2 reconciliation: compare plan sum vs master total (actual quantity only).
(B mode) Keep master aggregate rows (e.g., 관목계/교목계/초화류/식재지반 등) and
create synthetic plan aggregate rows by grouping plan items based on master table order.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Set, Tuple

from extract_master_table import choose_master_pdf, extract_master_rows
from extract_plan_tables import collect_plan_tables

SUMMARY_COLUMNS = [
    "work_name",
    "spec",
    "unit",
    "master_total_qty",
    "plan_total_qty",
    "diff",
    "status",
    "plan_sources",
    "plan_pages",
]

TOLERANCE = Decimal("0.001")


# ---------------------------
# helpers
# ---------------------------
def clean_key_text(value: str) -> str:
    value = (value or "").replace("\r", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", value).strip()


def parse_decimal(value: str) -> Decimal | None:
    text = clean_key_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def is_aggregate_name(name: str) -> bool:
    """Heuristic: master aggregate rows / group headers."""
    n = clean_key_text(name)
    if not n:
        return False
    # 대표적인 집계/구분 행 패턴
    if n.endswith("계"):
        return True
    if "면적" in n:
        return True
    # 총괄표에서 자주 나오는 구분/그룹 명(프로젝트마다 늘어날 수 있음)
    fixed = {
        "관목", "교목", "초화류", "식재지반", "잔디", "잔디면적",
        "낙엽관목", "상록관목", "낙엽교목", "상록교목",
        "및 기타", "기타",
        "구분", "기호",
    }
    if n in fixed:
        return True
    # '관목/교목'이 들어가면서 개별 수목명 같지 않은 경우(대체로 그룹 행)
    if ("관목" in n or "교목" in n) and len(n) <= 6:
        return True
    return False


def write_extract_log(logs, out_path: Path) -> None:
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
    recon_rows: list[dict[str, str]],
    plan_rows: list[dict[str, str]],
    master_rows: list[dict[str, str]],
) -> bool:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        return False

    # ✅ 빈 리스트여도 컬럼이 생기게 강제 (KeyError 방지)
    recon_df = pd.DataFrame(recon_rows, columns=SUMMARY_COLUMNS)

    summary_counts = Counter(recon_df["status"].tolist()) if len(recon_df) else Counter()
    plan_file_counts = Counter(row.get("source_pdf", "") for row in plan_rows)

    summary_table = pd.DataFrame(
        [{"metric": "total_recon_items", "value": len(recon_rows)}]
        + [{"metric": f"status_{k}", "value": v} for k, v in sorted(summary_counts.items())]
        + [{"metric": f"plan_rows_{k}", "value": v} for k, v in sorted(plan_file_counts.items())]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_table.to_excel(writer, sheet_name="Summary", index=False)

        # Mismatch / Only sheets
        if "status" in recon_df.columns and len(recon_df):
            recon_df[recon_df["status"] == "MISMATCH"].to_excel(
                writer, sheet_name="Mismatches", index=False
            )
            recon_df[recon_df["status"] == "ONLY_IN_MASTER"].to_excel(
                writer, sheet_name="OnlyInMaster", index=False
            )
            recon_df[recon_df["status"] == "ONLY_IN_PLANS"].to_excel(
                writer, sheet_name="OnlyInPlans", index=False
            )
        else:
            # 빈 시트라도 생성
            pd.DataFrame(columns=SUMMARY_COLUMNS).to_excel(writer, sheet_name="Mismatches", index=False)
            pd.DataFrame(columns=SUMMARY_COLUMNS).to_excel(writer, sheet_name="OnlyInMaster", index=False)
            pd.DataFrame(columns=SUMMARY_COLUMNS).to_excel(writer, sheet_name="OnlyInPlans", index=False)

        pd.DataFrame(plan_rows).to_excel(writer, sheet_name="RawPlanExtract", index=False)
        pd.DataFrame(master_rows).to_excel(writer, sheet_name="RawMasterExtract", index=False)

    return True


# ---------------------------
# B-mode grouping logic
# ---------------------------
def build_master_groups(master_rows: list[dict[str, str]]) -> Dict[str, Set[str]]:
    """
    Build group -> set(species/work_name) based on master table order.
    Example:
        관목계
          철쭉
          회양목
        교목계
          느티나무
    """
    groups: Dict[str, Set[str]] = defaultdict(set)
    current_group: str | None = None

    for r in master_rows:
        name = clean_key_text(r.get("work_name", ""))
        if not name:
            continue

        if is_aggregate_name(name):
            current_group = name
            # 그룹 자신은 멤버로 넣지 않음
            continue

        # 개별 수목/항목 -> 현재 그룹에 귀속
        if current_group:
            groups[current_group].add(name)

    return groups


def reconcile(
    plan_rows: list[dict[str, str]],
    master_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    # 1) plan 기본 집계: (work_name,spec,unit) -> qty
    plan_map: dict[tuple[str, str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    plan_sources: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    plan_pages: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    # work_name 단위로도 접근하기 위해 별도 인덱스
    plan_by_name: dict[str, list[tuple[tuple[str, str, str], Decimal]]] = defaultdict(list)

    for row in plan_rows:
        key = (
            clean_key_text(row.get("work_name", "")),
            clean_key_text(row.get("spec", "")),
            clean_key_text(row.get("unit", "")),
        )
        qty = parse_decimal(row.get("qty", ""))
        if qty is None:
            continue

        plan_map[key] += qty
        plan_sources[key].add(row.get("source_pdf", ""))
        plan_pages[key].add(str(row.get("source_page", "")))

        if key[0]:
            plan_by_name[key[0]].append((key, qty))

    # 2) master map: (work_name,spec,unit) -> master_total_qty
    master_map: dict[tuple[str, str, str], Decimal] = {}
    master_key_by_name: dict[str, list[tuple[str, str, str]]] = defaultdict(list)

    for row in master_rows:
        key = (
            clean_key_text(row.get("work_name", "")),
            clean_key_text(row.get("spec", "")),
            clean_key_text(row.get("unit", "")),
        )
        qty = parse_decimal(row.get("master_total_qty", ""))
        if not any(key):
            continue
        if qty is None:
            qty = Decimal("0")
        master_map[key] = qty
        if key[0]:
            master_key_by_name[key[0]].append(key)

    # 3) B-mode: master 그룹(집계행) 만들고, plan을 그 그룹으로 합산하여 "가짜 plan 집계행" 생성
    master_groups = build_master_groups(master_rows)  # group_name -> set(member_name)

    # group_name이 실제로 master에 존재하는 키(집계행 키)를 찾고, 그 키로 plan 합계를 만든다
    for group_name, members in master_groups.items():
        # master에 동일 group_name 행이 없으면(드물지만) 스킵
        group_keys = master_key_by_name.get(group_name, [])
        if not group_keys:
            continue

        # group은 대개 spec/unit 비어있음. master에 있는 첫 키를 대표로 사용
        group_key = group_keys[0]

        # members에 속한 수목명들에 대해 plan qty 합산
        total = Decimal("0")
        srcs: Set[str] = set()
        pages: Set[str] = set()

        for m in members:
            for (k, q) in plan_by_name.get(m, []):
                total += q
                srcs.update(plan_sources.get(k, set()))
                pages.update(plan_pages.get(k, set()))

        # plan 쪽에 집계행 키로 값 주입 (기존값이 있으면 누적)
        if total != Decimal("0"):
            plan_map[group_key] += total
            plan_sources[group_key].update(srcs)
            plan_pages[group_key].update(pages)

    # 4) 비교 키 전체
    all_keys = sorted(set(plan_map.keys()) | set(master_map.keys()))

    results: list[dict[str, str]] = []
    for key in all_keys:
        work_name, spec, unit = key
        plan_qty = plan_map.get(key)
        master_qty = master_map.get(key)

        if master_qty is None:
            status = "ONLY_IN_PLANS"
            master_val = Decimal("0")
            plan_val = plan_qty or Decimal("0")
        elif plan_qty is None:
            status = "ONLY_IN_MASTER"
            master_val = master_qty
            plan_val = Decimal("0")
        else:
            master_val = master_qty
            plan_val = plan_qty
            diff = plan_val - master_val
            status = "OK" if abs(diff) <= TOLERANCE else "MISMATCH"

        diff_val = plan_val - master_val
        results.append(
            {
                "work_name": work_name,
                "spec": spec,
                "unit": unit,
                "master_total_qty": str(master_val),
                "plan_total_qty": str(plan_val),
                "diff": str(diff_val),
                "status": status,
                "plan_sources": ", ".join(sorted(s for s in plan_sources.get(key, set()) if s)),
                "plan_pages": ", ".join(sorted(p for p in plan_pages.get(key, set()) if p)),
            }
        )

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="실제수량 합계(집계행 포함) 검수 파이프라인")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--outdir", type=Path, default=Path("output"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    plan_rows, logs = collect_plan_tables(args.pdf_dir)
    master_pdf = choose_master_pdf(args.pdf_dir)
    master_rows = extract_master_rows(master_pdf) if master_pdf else []

    recon_rows = reconcile(plan_rows, master_rows)
    write_csv(recon_rows, args.outdir / "recon_summary.csv")
    excel_ok = write_excel(args.outdir / "recon_detail.xlsx", recon_rows, plan_rows, master_rows)
    write_extract_log(logs, args.outdir / "extract_log.txt")

    if not excel_ok:
        with (args.outdir / "extract_log.txt").open("a", encoding="utf-8") as f:
            f.write('"system",0,"",0,"recon_detail.xlsx 생성 스킵: pandas/openpyxl 미설치"\n')

    print(f"Plan rows: {len(plan_rows)}")
    print(f"Master rows: {len(master_rows)}")
    print(f"Recon rows: {len(recon_rows)}")


if __name__ == "__main__":
    main()
