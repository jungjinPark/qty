#!/usr/bin/env python3
"""Stage-2 reconciliation: compare plan sum vs master total (actual quantity only)."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

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


def clean_key_text(value: str) -> str:
    value = value.replace("\r", " ").replace("\n", " ")
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

    summary_counts = Counter(row["status"] for row in recon_rows)
    plan_file_counts = Counter(row["source_pdf"] for row in plan_rows)

    summary_table = pd.DataFrame(
        [{"metric": "total_recon_items", "value": len(recon_rows)}]
        + [{"metric": f"status_{k}", "value": v} for k, v in sorted(summary_counts.items())]
        + [{"metric": f"plan_rows_{k}", "value": v} for k, v in sorted(plan_file_counts.items())]
    )

    recon_df = pd.DataFrame(recon_rows)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_table.to_excel(writer, sheet_name="Summary", index=False)
        recon_df[recon_df["status"] == "MISMATCH"].to_excel(
            writer, sheet_name="Mismatches", index=False
        )
        recon_df[recon_df["status"] == "ONLY_IN_MASTER"].to_excel(
            writer, sheet_name="OnlyInMaster", index=False
        )
        recon_df[recon_df["status"] == "ONLY_IN_PLANS"].to_excel(
            writer, sheet_name="OnlyInPlans", index=False
        )
        pd.DataFrame(plan_rows).to_excel(writer, sheet_name="RawPlanExtract", index=False)
        pd.DataFrame(master_rows).to_excel(writer, sheet_name="RawMasterExtract", index=False)

    return True


def reconcile(plan_rows: list[dict[str, str]], master_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    plan_map: dict[tuple[str, str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    plan_sources: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    plan_pages: dict[tuple[str, str, str], set[str]] = defaultdict(set)

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

    master_map: dict[tuple[str, str, str], Decimal] = {}
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

    all_keys = sorted(set(plan_map.keys()) | set(master_map.keys()))

    results: list[dict[str, str]] = []
    for key in all_keys:
        work_name, spec, unit = key
        plan_qty = plan_map.get(key)
        master_qty = master_map.get(key)

        if master_qty is None:
            status = "ONLY_IN_PLANS"
            master_val = Decimal("0")
            plan_val = plan_qty
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
    parser = argparse.ArgumentParser(description="실제수량 합계 검수 파이프라인")
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
