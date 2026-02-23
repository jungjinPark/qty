# qty

조경 도면 PDF에서 수량표를 추출하고, 총괄수량표(L-003)의 **합계(실제수량)** 와 계획도 합계를 대조하는 파이프라인입니다.

## 실행

```bash
python scripts/reconcile_totals.py --pdf_dir . --outdir output
```

## 스크립트 구성

- `scripts/extract_plan_tables.py`: 계획도(`수량표` 포함 페이지) 추출
- `scripts/extract_master_table.py`: 총괄수량표(`총괄수량표` 또는 `L-003`) 추출
- `scripts/reconcile_totals.py`: 항목별 합산/검증 및 결과물 생성

## 출력물

- `output/recon_summary.csv`
  - 컬럼: `work_name,spec,unit,master_total_qty,plan_total_qty,diff,status,plan_sources,plan_pages`
- `output/recon_detail.xlsx` (pandas/openpyxl 설치 시)
  - 시트: `Summary`, `Mismatches`, `OnlyInMaster`, `OnlyInPlans`, `RawPlanExtract`, `RawMasterExtract`
- `output/extract_log.txt`
  - 추출 로그 (파일, 페이지, 표제목, 행수, 실패사유)

## 검증 규칙 (2단계)

- 비교 키: `(work_name, spec, unit)` 완전일치 (트림/다중공백 정리만 적용)
- 계획도 합계: `plan_total_qty = Σ qty`
- 차이: `diff = plan_total_qty - master_total_qty`
- 허용 오차: `±0.001`
- 상태값:
  - `OK`
  - `MISMATCH`
  - `ONLY_IN_MASTER`
  - `ONLY_IN_PLANS`

> 주의: 본 단계에서는 인정수량/2주인정/인정수량 제외 등 법적 수량 로직을 처리하지 않습니다.
