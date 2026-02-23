#!/usr/bin/env python3
"""Stage-3 reconciliation: validate recognized quantity for tree items."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from extract_master_table import choose_master_pdf
from extract_plan_tables import ExtractLog, collect_plan_tables

TOLERANCE = Decimal("0.001")

SUMMARY_COLUMNS = [
    "work_name",
    "spec",
    "unit",
    "actual_qty",
    "factor",
    "expected_recognized_qty",
    "recognized_qty_in_master",
    "diff",
    "status",
    "remark",
    "sources",
    "pages",
]

TREE_STRONG_KEYWORDS = ["교목", "수목", "식재", "흉고", "근원", "R", "H"]
TREE_WEAK_KEYWORDS = ["수목명", "관목", "주"]
EXCLUDE_PATTERN = re.compile(r"인정\s*수량\s*제외|인정\s*제외|산입\s*제외")
FACTOR_PATTERN = re.compile(r"(\d+)\s*주\s*인정")
SUMMARY_FILE_PATTERN = re.compile(r"총괄수량표|L-?003", re.IGNORECASE)

MASTER_COLUMN_CANDIDATES = {
    "work_name": ["공종", "품명", "명칭", "항목"],
    "spec": ["규격", "사양", "치수"],
    "unit": ["단위"],
    "master_total_qty": ["합계", "실제수량", "총수량", "수량", "물량"],
    "recognized_qty": ["인정수량", "법적수량", "인정", "법적"],
    "remark": ["비고", "참고"],
}


@dataclass
class RuleResult:
    status: str
    factor: Decimal | None
    note: str


def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\r", " ").replace("\n", " ")).strip()


def parse_decimal(value: str | None) -> Decimal | None:
    text = clean_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None


def is_master_pdf(path: Path) -> bool:
    return bool(SUMMARY_FILE_PATTERN.search(path.name))


def iter_candidate_tables(page):
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
            cleaned = tuple(tuple(clean_text(cell) for cell in row) for row in table if row)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            yield [list(row) for row in cleaned]


def find_header_map(table: list[list[str]]) -> tuple[int, dict[str, int]]:
    for idx, row in enumerate(table[:6]):
        mapping: dict[str, int] = {}
        for c_idx, cell in enumerate(row):
            for key, options in MASTER_COLUMN_CANDIDATES.items():
                if key in mapping:
                    continue
                if any(opt in cell for opt in options):
                    mapping[key] = c_idx
        if "work_name" in mapping and "master_total_qty" in mapping:
            return idx, mapping
    return -1, {}


def extract_master_rows_extended(pdf_dir: Path) -> tuple[list[dict[str, str]], list[ExtractLog]]:
    rows: list[dict[str, str]] = []
    logs: list[ExtractLog] = []
    master_pdf = choose_master_pdf(pdf_dir)
    if not master_pdf:
        logs.append(ExtractLog("", 0, "", 0, "총괄수량표 PDF 미발견"))
        return rows, logs

    import pdfplumber  # type: ignore

    try:
        with pdfplumber.open(master_pdf) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                if "총괄" not in page_text and "수량표" not in page_text:
                    continue

                found = False
                for table in iter_candidate_tables(page):
                    header_idx, header_map = find_header_map(table)
                    if header_idx < 0:
                        continue

                    carry_work = ""
                    table_count = 0
                    for raw in table[header_idx + 1 :]:
                        row = [clean_text(c) for c in raw]
                        if not any(row):
                            continue

                        def get_cell(name: str) -> str:
                            pos = header_map.get(name, -1)
                            if pos < 0 or pos >= len(row):
                                return ""
                            return clean_text(row[pos])

                        rec = {
                            "work_name": get_cell("work_name"),
                            "spec": get_cell("spec"),
                            "unit": get_cell("unit"),
                            "master_total_qty": get_cell("master_total_qty"),
                            "recognized_qty": get_cell("recognized_qty"),
                            "remark": get_cell("remark"),
                            "source_pdf": master_pdf.name,
                            "source_page": str(page_idx),
                        }
                        if not rec["work_name"] and carry_work:
                            rec["work_name"] = carry_work
                        if rec["work_name"]:
                            carry_work = rec["work_name"]

                        if not rec["work_name"] and not rec["master_total_qty"] and not rec["recognized_qty"]:
                            continue

                        rows.append(rec)
                        table_count += 1

                    found = found or table_count > 0
                    if table_count > 0:
                        logs.append(ExtractLog(master_pdf.name, page_idx, "총괄수량표", table_count, ""))

                if not found:
                    logs.append(
                        ExtractLog(master_pdf.name, page_idx, "총괄수량표", 0, "유효 헤더/행 미탐지")
                    )
    except Exception as exc:  # noqa: BLE001
        logs.append(ExtractLog(master_pdf.name, 0, "", 0, f"PDF 처리 실패: {exc}"))

    return rows, logs


def load_stage2_actual_map(outdir: Path) -> dict[tuple[str, str, str], Decimal]:
    recon_csv = outdir / "recon_summary.csv"
    actual_map: dict[tuple[str, str, str], Decimal] = {}
    if not recon_csv.exists():
        return actual_map

    with recon_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (clean_text(row.get("work_name")), clean_text(row.get("spec")), clean_text(row.get("unit")))
            qty = parse_decimal(row.get("master_total_qty") or row.get("plan_total_qty"))
            if qty is not None:
                actual_map[key] = qty
    return actual_map


def detect_tree_status(row: dict[str, str], plan_rows: list[dict[str, str]]) -> str:
    text = " ".join(
        [
            row.get("work_name", ""),
            row.get("spec", ""),
            row.get("remark", ""),
        ]
    )
    plan_texts = [
        " ".join(
            [
                r.get("work_name", ""),
                r.get("spec", ""),
                r.get("remark", ""),
                r.get("trade_category", ""),
            ]
        )
        for r in plan_rows
        if clean_text(r.get("work_name")) == clean_text(row.get("work_name"))
        and clean_text(r.get("spec")) == clean_text(row.get("spec"))
        and clean_text(r.get("unit")) == clean_text(row.get("unit"))
    ]
    full_text = " ".join([text] + plan_texts)

    if any(k in full_text for k in TREE_STRONG_KEYWORDS):
        return "TREE"
    if any(k in full_text for k in TREE_WEAK_KEYWORDS):
        return "TREE_CANDIDATE"
    return "NON_TREE"


def parse_rule(remark: str) -> RuleResult:
    remark_clean = clean_text(remark)
    if not remark_clean:
        return RuleResult("RULE_NOT_FOUND", None, "remark_empty")

    if EXCLUDE_PATTERN.search(remark_clean):
        return RuleResult("EXCLUDED", None, "exclude_keyword")

    factors = [Decimal(m.group(1)) for m in FACTOR_PATTERN.finditer(remark_clean)]
    if not factors:
        return RuleResult("RULE_NOT_FOUND", None, "factor_not_found")

    max_factor = max(factors)
    if len(set(factors)) > 1:
        return RuleResult("RULE_FOUND", max_factor, f"factor_conflict:{','.join(str(f) for f in factors)}")
    return RuleResult("RULE_FOUND", max_factor, f"factor:{max_factor}")


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


def write_rule_log(lines: list[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("index,work_name,spec,unit,status,rule_note,remark\n")
        for line in lines:
            f.write(line + "\n")


def write_excel(out_path: Path, rows: list[dict[str, str]], raw_rows: list[dict[str, str]]) -> bool:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        return False

    df = pd.DataFrame(rows)
    summary_counts = Counter(df["status"]) if not df.empty else {}
    factor_counts = Counter(df["factor"]) if not df.empty else {}
    summary_data = [{"metric": "total_items", "value": len(rows)}]
    summary_data += [{"metric": f"status_{k}", "value": int(v)} for k, v in sorted(summary_counts.items())]
    summary_data += [{"metric": f"factor_{k}", "value": int(v)} for k, v in sorted(factor_counts.items())]

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)
        df[df["status"] == "MISMATCH"].to_excel(writer, sheet_name="Mismatches", index=False)
        df[df["status"] == "EXCLUDED"].to_excel(writer, sheet_name="Excluded", index=False)
        df[df["status"] == "RULE_NOT_FOUND"].to_excel(writer, sheet_name="RuleNotFound", index=False)
        df[df["status"] == "TREE_CANDIDATE"].to_excel(writer, sheet_name="TreeCandidate", index=False)
        pd.DataFrame(raw_rows).to_excel(writer, sheet_name="RawExtract", index=False)

    return True


def reconcile_recognized(
    master_rows: list[dict[str, str]],
    plan_rows: list[dict[str, str]],
    stage2_actual_map: dict[tuple[str, str, str], Decimal],
) -> tuple[list[dict[str, str]], list[str], list[dict[str, str]]]:
    results: list[dict[str, str]] = []
    rule_logs: list[str] = []
    raw_extract: list[dict[str, str]] = []

    # Same-key plan source/page aggregation for report provenance
    plan_sources: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    plan_pages: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for p in plan_rows:
        key = (clean_text(p.get("work_name")), clean_text(p.get("spec")), clean_text(p.get("unit")))
        plan_sources[key].add(clean_text(p.get("source_pdf")))
        plan_pages[key].add(clean_text(p.get("source_page")))

    for idx, row in enumerate(master_rows, start=1):
        key = (clean_text(row.get("work_name")), clean_text(row.get("spec")), clean_text(row.get("unit")))
        tree_status = detect_tree_status(row, plan_rows)
        if tree_status == "NON_TREE":
            continue

        actual_qty = stage2_actual_map.get(key)
        if actual_qty is None:
            actual_qty = parse_decimal(row.get("master_total_qty")) or Decimal("0")

        recognized_qty = parse_decimal(row.get("recognized_qty"))
        rule = parse_rule(row.get("remark", ""))
        status = ""
        factor_val: Decimal | None = None
        expected: Decimal | None = None

        if tree_status == "TREE_CANDIDATE":
            status = "TREE_CANDIDATE"
        elif rule.status == "EXCLUDED":
            status = "EXCLUDED"
        elif rule.status == "RULE_NOT_FOUND":
            status = "RULE_NOT_FOUND"
        else:
            factor_val = rule.factor
            expected = (actual_qty * factor_val) if factor_val is not None else None
            if recognized_qty is None:
                status = "MISMATCH"
            else:
                diff = expected - recognized_qty if expected is not None else Decimal("0")
                status = "OK" if abs(diff) <= TOLERANCE else "MISMATCH"

        diff_val = ""
        if expected is not None and recognized_qty is not None:
            diff_val = str(expected - recognized_qty)

        source_set = {clean_text(row.get("source_pdf"))}
        source_set |= {s for s in plan_sources.get(key, set()) if s}
        page_set = {clean_text(row.get("source_page"))}
        page_set |= {p for p in plan_pages.get(key, set()) if p}

        res = {
            "work_name": key[0],
            "spec": key[1],
            "unit": key[2],
            "actual_qty": str(actual_qty),
            "factor": "" if factor_val is None else str(factor_val),
            "expected_recognized_qty": "" if expected is None else str(expected),
            "recognized_qty_in_master": "" if recognized_qty is None else str(recognized_qty),
            "diff": diff_val,
            "status": status,
            "remark": clean_text(row.get("remark")),
            "sources": ", ".join(sorted(s for s in source_set if s)),
            "pages": ", ".join(sorted(p for p in page_set if p)),
        }
        results.append(res)
        raw_extract.append(
            {
                "work_name": key[0],
                "spec": key[1],
                "unit": key[2],
                "master_total_qty": clean_text(row.get("master_total_qty")),
                "recognized_qty": clean_text(row.get("recognized_qty")),
                "remark": clean_text(row.get("remark")),
                "tree_status": tree_status,
                "source_pdf": clean_text(row.get("source_pdf")),
                "source_page": clean_text(row.get("source_page")),
            }
        )
        rule_logs.append(
            f'{idx},"{key[0]}","{key[1]}","{key[2]}","{status}","{rule.note}","{clean_text(row.get("remark"))}"'
        )

    return results, rule_logs, raw_extract


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="3단계 인정수량 검수")
    parser.add_argument("--pdf_dir", type=Path, default=Path("."))
    parser.add_argument("--outdir", type=Path, default=Path("output"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    plan_rows, plan_logs = collect_plan_tables(args.pdf_dir)
    master_rows, master_logs = extract_master_rows_extended(args.pdf_dir)
    stage2_actual_map = load_stage2_actual_map(args.outdir)

    recognized_rows, rule_logs, raw_rows = reconcile_recognized(master_rows, plan_rows, stage2_actual_map)
    write_csv(recognized_rows, args.outdir / "recognized_summary.csv")
    write_rule_log(rule_logs, args.outdir / "recognized_log.txt")
    write_extract_log(plan_logs + master_logs, args.outdir / "extract_log.txt")
    excel_ok = write_excel(args.outdir / "recognized_detail.xlsx", recognized_rows, raw_rows)

    if not excel_ok:
        with (args.outdir / "extract_log.txt").open("a", encoding="utf-8") as f:
            f.write('"system",0,"",0,"recognized_detail.xlsx 생성 스킵: pandas/openpyxl 미설치"\n')

    print(f"Plan rows: {len(plan_rows)}")
    print(f"Master rows: {len(master_rows)}")
    print(f"Recognized rows: {len(recognized_rows)}")


if __name__ == "__main__":
    main()
