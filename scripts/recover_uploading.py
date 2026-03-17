#!/usr/bin/env python3
"""uploading 상태 파일들을 archive로 이동하고 completed로 업데이트하는 복구 스크립트."""

import shutil
from pathlib import Path
from datetime import datetime
import duckdb

db_path = "/data/pipeline.duckdb"
archive_base = Path("/nas/archive")
incoming_base = Path("/nas/incoming")

con = duckdb.connect(db_path)

# uploading 상태 파일 조회
rows = con.execute("""
    SELECT asset_id, source_path 
    FROM raw_files 
    WHERE ingest_status = 'uploading'
""").fetchall()

print(f"uploading 상태 파일: {len(rows)}개")

if not rows:
    con.close()
    print("복구할 파일 없음")
    exit(0)

# source_unit 분석 (폴더 구조 유지: AX지원사업/260213_MCC_Construction)
sample_path = Path(rows[0][1])
rel = sample_path.relative_to(incoming_base)
if len(rel.parts) >= 3:
    # 2단계 이상 중첩: AX지원사업/260213_MCC_Construction/file.mp4
    source_unit_name = str(Path(rel.parts[0]) / rel.parts[1])
    source_unit_path = incoming_base / rel.parts[0] / rel.parts[1]
elif len(rel.parts) >= 2:
    # 1단계 폴더: folder/file.mp4
    source_unit_name = rel.parts[0]
    source_unit_path = incoming_base / rel.parts[0]
else:
    source_unit_name = rel.parts[0]
    source_unit_path = incoming_base / rel.parts[0]

print(f"source_unit_name: {source_unit_name}")
print(f"source_unit_path: {source_unit_path}")
print(f"source_unit exists: {source_unit_path.exists()}")

# archive 디렉토리 생성
archive_unit_dir = archive_base / source_unit_name
idx = 2
while archive_unit_dir.exists():
    archive_unit_dir = archive_base / f"{source_unit_name}__{idx}"
    idx += 1

print(f"archive_unit_dir: {archive_unit_dir}")
archive_unit_dir.parent.mkdir(parents=True, exist_ok=True)

# 폴더 단위 이동 시도
try:
    shutil.move(str(source_unit_path), str(archive_unit_dir))
    print(f"폴더 이동 성공: {source_unit_path} -> {archive_unit_dir}")
    
    # DB 업데이트
    moved_count = 0
    for asset_id, source_path in rows:
        src = Path(source_path)
        try:
            rel_in_unit = src.relative_to(source_unit_path)
        except ValueError:
            rel_in_unit = Path(src.name)
        archive_path = archive_unit_dir / rel_in_unit
        
        if archive_path.exists():
            con.execute("""
                UPDATE raw_files 
                SET ingest_status = 'completed', 
                    archive_path = ?,
                    raw_bucket = COALESCE(raw_bucket, 'vlm-raw'),
                    updated_at = ?
                WHERE asset_id = ?
            """, [str(archive_path), datetime.now(), asset_id])
            moved_count += 1
        else:
            print(f"  Warning: archive 파일 없음: {archive_path}")
    
    print(f"DB 업데이트 완료: {moved_count}개")
    
except Exception as e:
    print(f"폴더 이동 실패: {e}")
    print("파일 단위 이동으로 전환...")
    
    archive_unit_dir.mkdir(parents=True, exist_ok=True)
    moved_count = 0
    
    for asset_id, source_path in rows:
        src = Path(source_path)
        if not src.exists():
            print(f"  Skip: 소스 없음: {src}")
            continue
        
        try:
            rel_in_unit = src.relative_to(source_unit_path)
        except ValueError:
            rel_in_unit = Path(src.name)
        
        dest = archive_unit_dir / rel_in_unit
        dest.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            shutil.move(str(src), str(dest))
            con.execute("""
                UPDATE raw_files 
                SET ingest_status = 'completed', 
                    archive_path = ?,
                    raw_bucket = COALESCE(raw_bucket, 'vlm-raw'),
                    updated_at = ?
                WHERE asset_id = ?
            """, [str(dest), datetime.now(), asset_id])
            moved_count += 1
        except Exception as e2:
            print(f"  파일 이동 실패: {src}: {e2}")
    
    print(f"파일 단위 이동 완료: {moved_count}개")

con.close()
print("복구 완료")
