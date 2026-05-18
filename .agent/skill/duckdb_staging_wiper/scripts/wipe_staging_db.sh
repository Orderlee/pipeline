#!/bin/bash
# wipe_staging_db.sh - Staging DuckDB 초기화 스크립트

DB_PATH="/home/user/work_p/Datapipeline-Data-data_pipeline/docker/data/staging.duckdb"
WAL_PATH="${DB_PATH}.wal"

echo "[INFO] Staging DuckDB 초기화를 시작합니다..."

# 1. 파일 존재 확인
if [ ! -f "$DB_PATH" ]; then
    echo "[WARN] 삭제할 DB 파일이 존재하지 않습니다: $DB_PATH"
else
    # 2. 파일 삭제 시도
    echo "[INFO] 삭제 중: $DB_PATH"
    rm -f "$DB_PATH"
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] DB 파일이 삭제되었습니다."
    else
        echo "[ERROR] DB 파일 삭제 실패! 권한을 확인하세요 (sudo 필요할 수 있음)."
    fi
fi

# 3. WAL 파일 삭제
if [ -f "$WAL_PATH" ]; then
    echo "[INFO] WAL 파일 삭제 중: $WAL_PATH"
    rm -f "$WAL_PATH"
fi

# 4. 최종 확인
if [ ! -f "$DB_PATH" ]; then
    echo "[FINISH] Staging DB가 성공적으로 비워졌습니다."
else
    echo "[FAIL] DB 파일이 여전히 존재합니다."
    exit 1
fi
