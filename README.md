# qty

조경 도면 PDF에서 수량표를 자동 추출하고,  
총괄수량표(L-003)의 실제수량 및 교목 인정수량을 자동 검수하는 파이프라인입니다.

---

# 개요

본 프로젝트는 다음 3단계 검수 프로세스를 자동화합니다.

1. 계획도 PDF에서 수량표 자동 추출
2. 총괄수량표(L-003)의 **합계(실제수량)** 검증
3. 교목 대상 **인정수량(비고 규칙 기반)** 검증

---

# 실행

## 2단계: 실제수량(합계) 검수

```bash
python scripts/reconcile_totals.py --pdf_dir . --outdir output

생성 파일
	•	output/recon_summary.csv
	•	output/recon_detail.xlsx
	•	output/extract_log.txt

⸻

3단계: 인정수량(교목) 검수

python scripts/reconcile_recognized_qty.py --pdf_dir . --outdir output

생성 파일
	•	output/recognized_summary.csv
	•	output/recognized_detail.xlsx
	•	output/recognized_log.txt

⸻

스크립트 구성
	•	scripts/extract_plan_tables.py
→ 계획도(수량표 포함 페이지) 추출
	•	scripts/extract_master_table.py
→ 총괄수량표(L-003) 추출
	•	scripts/reconcile_totals.py
→ 항목별 합산 및 실제수량 검증
	•	scripts/reconcile_recognized_qty.py
→ 교목 인정수량 규칙 추출 및 검증

⸻

출력물

2단계 출력

output/recon_summary.csv

컬럼:

work_name, spec, unit,
master_total_qty, plan_total_qty, diff,
status, plan_sources, plan_pages

output/recon_detail.xlsx

시트:
	•	Summary
	•	Mismatches
	•	OnlyInMaster
	•	OnlyInPlans
	•	RawPlanExtract
	•	RawMasterExtract

⸻

3단계 출력

output/recognized_summary.csv

컬럼:

work_name, spec, unit,
actual_qty, factor,
expected_recognized_qty,
recognized_qty_in_master,
diff, status, remark,
sources, pages

output/recognized_detail.xlsx

시트:
	•	Summary
	•	Mismatches
	•	Excluded
	•	RuleNotFound
	•	TreeCandidate
	•	RawExtract

output/recognized_log.txt
	•	인정 제외 / 주인정 / 미탐지 규칙 로그

⸻

검증 규칙 (2단계)
	•	비교 키: (work_name, spec, unit) 완전일치
(트림/다중공백 정리 적용)
	•	계획도 합계:

plan_total_qty = Σ qty


	•	차이 계산:

diff = plan_total_qty - master_total_qty


	•	허용 오차:

±0.001



상태값
	•	OK
	•	MISMATCH
	•	ONLY_IN_MASTER
	•	ONLY_IN_PLANS

※ 2단계에서는 인정수량/주인정/제외 등 법적 수량 로직을 처리하지 않습니다.

⸻

검증 규칙 (3단계)

대상
	•	교목(또는 교목 후보) 항목

실제수량 (actual_qty) 우선순위
	1.	output/recon_summary.csv의 master_total_qty
	2.	총괄수량표에서 직접 추출한 값

⸻

비고(remark) 규칙
	•	인정수량 제외 / 인정 제외 / 산입 제외
→ EXCLUDED
	•	(\d+)주인정

factor = N
expected_recognized_qty = actual_qty × N


	•	규칙 미탐지
→ RULE_NOT_FOUND

⸻

인정수량 검증

총괄수량표에 인정수량 값이 존재할 경우:

diff = expected_recognized_qty - recognized_qty_in_master

허용 오차:

±0.001


⸻

3단계 상태값
	•	OK
	•	MISMATCH
	•	EXCLUDED
	•	RULE_NOT_FOUND
	•	TREE_CANDIDATE

⸻

전제 및 주의사항
	•	프로젝트마다 층 구성은 다를 수 있음
(지상층/옥상층/2층/3층/B1 등)
	•	동일 층이 여러 장의 도면으로 구성될 수 있음
	•	공종명은 동일해야 함 (완전일치 매칭)
	•	규격 체계 정규화는 수행하지 않음 (공백 정리 정도만 적용)
	•	금액 관련 필드는 모두 제외

⸻

처리 흐름
	1.	계획도 PDF → 수량표 추출
	2.	총괄수량표(L-003) → 실제수량 검증
	3.	교목 비고 규칙 분석 → 인정수량 검증
	4.	CSV / XLSX 리포트 생성

---

# 🔵 적용 방법

1. GitHub → `README.md`
2. 연필 아이콘 클릭
3. 기존 내용 **전부 삭제**
4. 위 내용 **전체 붙여넣기**
5. Commit message:

Refactor README - full stage2/3 pipeline documentation

6. Commit changes
