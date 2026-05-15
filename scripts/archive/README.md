# scripts/archive/

일회성 운영 작업 / 마이그레이션 / cleanup 스크립트의 보관 디렉토리.

여기 들어간 파일들은 **이미 실행 완료된 일회성 작업**이라 일상적 운영에서 호출되지 않는다.
필요 시 git history 와 함께 참고용으로 유지한다.

## 보관 파일

| 파일 | 원래 위치 | 용도 |
|---|---|---|
| `cleanup_20260402.py` | scripts/ | 2026-04-02 DB 정리 일회성 작업 (완료) |
| `cleanup_minio_20260402.py` | scripts/ | 2026-04-02 MinIO 객체 정리 일회성 작업 (완료) |
| `migrate_legacy/profile.py` | scripts/migrate_legacy/ | 제거된 schema 의 프로파일 마이그레이션 (완료) |
| `migrate_legacy/build_pilot_manifest.py` | scripts/migrate_legacy/ | 초기 pilot manifest 생성 (완료) |

## 정책

- **추가 시**: 파일 이름에 날짜(YYYYMMDD) 또는 명확한 일회성 표시 + 완료 시점 README 표 추가
- **제거 시**: git history 만으로 충분하다고 판단되면 삭제. 단 1년 이상 archive 에 머문 파일만.
