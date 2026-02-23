# qty

조경 도면 PDF에서 `수량표`를 자동 탐지해 CSV로 추출하는 스크립트입니다.

## 사용법

```bash
python3 scripts/extract_qty_tables.py --input-dir <PDF_디렉터리> --output-dir extracted
```

기본값:
- `--input-dir .`
- `--output-dir extracted`

## 출력물

- `extracted/plan_tables.csv`: 모든 계획도 수량표 통합
- `extracted/summary_table.csv`: 총괄수량표만 분리
- `extracted/extract_log.txt`: 파일별 추출 로그
