#!/usr/bin/env python3
"""
NAS 폴더 트리 구조를 PostgreSQL에 저장하여 Grafana에서 시각화

사용법:
    # datapipeline 컨테이너 내부에서 실행
    python nas_folder_tree_to_postgres.py                         # 기본 경로 스캔
    python nas_folder_tree_to_postgres.py --path /nas/datasets    # 특정 경로 스캔
    python nas_folder_tree_to_postgres.py --max-depth 3           # 깊이 제한
    python nas_folder_tree_to_postgres.py --dry-run               # 미리보기만

Docker 실행:
    docker exec pipeline-app-1 python3 /src/python/nas_folder_tree_to_postgres.py --fast
"""

import os
import argparse
import glob
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from tqdm import tqdm

# 기본 설정
DEFAULT_NAS_PATHS = [
    "/nas/datasets/projects/vietnam_data/organized_videos/*_weapon",
    "/nas/datasets/projects/vietnam_data/processed_data/*_weapon",
]
DEFAULT_MAX_DEPTH = 10

# PostgreSQL 기본 설정
DEFAULT_PG_HOST = "pipeline-postgres"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DB = "nas_tree"
DEFAULT_PG_USER = "airflow"
DEFAULT_PG_PASSWORD = "airflow"
DEFAULT_PG_ADMIN_DB = "postgres"


def get_postgres_connection(host: str, port: int, database: str, user: str, password: str):
    """PostgreSQL 연결"""
    ensure_database_exists(host, port, database, user, password)
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )


def ensure_database_exists(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    admin_db: str = DEFAULT_PG_ADMIN_DB,
):
    """대상 DB가 없으면 생성"""
    if not database or database == admin_db:
        return

    admin_conn = psycopg2.connect(
        host=host,
        port=port,
        database=admin_db,
        user=user,
        password=password,
    )
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                print(f"✅ PostgreSQL DB 생성 완료: {database}")
    finally:
        admin_conn.close()


def ensure_schema(conn):
    """Grafana용 폴더 트리 테이블 스키마 생성"""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS nas_folder_tree (
        id SERIAL PRIMARY KEY,
        scan_time TIMESTAMP DEFAULT NOW(),
        path TEXT NOT NULL,
        parent_path TEXT,
        name TEXT NOT NULL,
        depth INTEGER DEFAULT 0,
        node_type TEXT DEFAULT 'folder',
        size_bytes BIGINT DEFAULT 0,
        file_count INTEGER DEFAULT 0,
        folder_count INTEGER DEFAULT 0,
        total_size_bytes BIGINT DEFAULT 0,
        total_file_count INTEGER DEFAULT 0,
        tree_prefix TEXT DEFAULT '',
        sort_key TEXT DEFAULT ''
    );
    
    DO $$ 
    BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='nas_folder_tree' AND column_name='tree_prefix') THEN
            ALTER TABLE nas_folder_tree ADD COLUMN tree_prefix TEXT DEFAULT '';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='nas_folder_tree' AND column_name='sort_key') THEN
            ALTER TABLE nas_folder_tree ADD COLUMN sort_key TEXT DEFAULT '';
        END IF;
    END $$;

    CREATE TABLE IF NOT EXISTS nas_folder_extensions (
        id SERIAL PRIMARY KEY,
        scan_time TIMESTAMP DEFAULT NOW(),
        folder_path TEXT NOT NULL,
        extension TEXT NOT NULL,
        file_count INTEGER DEFAULT 0,
        total_size_bytes BIGINT DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS nas_folder_summary (
        id SERIAL PRIMARY KEY,
        scan_time TIMESTAMP DEFAULT NOW(),
        root_path TEXT NOT NULL,
        total_folders INTEGER DEFAULT 0,
        total_files INTEGER DEFAULT 0,
        total_size_bytes BIGINT DEFAULT 0,
        total_size_gb DOUBLE PRECISION,
        max_depth INTEGER DEFAULT 0,
        unique_extensions INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS nas_timeseries (
        time TIMESTAMP DEFAULT NOW(),
        metric_name TEXT NOT NULL,
        metric_value DOUBLE PRECISION,
        labels JSONB
    );

    CREATE INDEX IF NOT EXISTS idx_nas_folder_tree_path ON nas_folder_tree(path);
    CREATE INDEX IF NOT EXISTS idx_nas_folder_tree_scan ON nas_folder_tree(scan_time);
    CREATE INDEX IF NOT EXISTS idx_nas_folder_extensions_path ON nas_folder_extensions(folder_path);
    CREATE INDEX IF NOT EXISTS idx_nas_timeseries_time ON nas_timeseries(time);
    """
    
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    print("✅ PostgreSQL 스키마 준비 완료")


def calculate_tree_prefixes(folder_tree: List[Dict]) -> List[Dict]:
    """폴더 트리에 트리 구조 prefix 계산"""
    if not folder_tree:
        return folder_tree
    
    sorted_tree = sorted(folder_tree, key=lambda x: x["path"])
    
    children_by_parent = defaultdict(list)
    for item in sorted_tree:
        parent = item["parent_path"] or ""
        children_by_parent[parent].append(item["path"])
    
    is_last_child = {}
    for parent, children in children_by_parent.items():
        sorted_children = sorted(children)
        for i, child in enumerate(sorted_children):
            is_last_child[child] = (i == len(sorted_children) - 1)
    
    path_to_item = {item["path"]: item for item in sorted_tree}
    
    for item in sorted_tree:
        path = item["path"]
        depth = item["depth"]
        
        if depth == 0:
            item["tree_prefix"] = ""
            item["sort_key"] = "0"
            continue
        
        if is_last_child.get(path, False):
            current_prefix = "└── "
        else:
            current_prefix = "├── "
        
        ancestor_prefix = ""
        current_path = item["parent_path"]
        
        ancestors = []
        while current_path and current_path in path_to_item:
            parent_item = path_to_item[current_path]
            if parent_item["depth"] > 0:
                if is_last_child.get(current_path, False):
                    ancestors.append("    ")
                else:
                    ancestors.append("│   ")
            current_path = parent_item["parent_path"]
        
        ancestor_prefix = "".join(reversed(ancestors))
        item["tree_prefix"] = ancestor_prefix + current_prefix
        
        sort_parts = []
        for part in path.split('/'):
            padded = re.sub(r'(\d+)', lambda m: m.group(1).zfill(10), part)
            sort_parts.append(padded)
        item["sort_key"] = '/'.join(sort_parts)
    
    return sorted_tree


def scan_folder_tree_fast(
    root_path: str,
    max_depth: int = DEFAULT_MAX_DEPTH,
    show_progress: bool = True,
    calculate_size: bool = True
) -> Tuple[List[Dict], Dict[str, Dict], Dict[str, int]]:
    """NAS 폴더 트리 구조 빠른 스캔 (os.walk 사용)"""
    
    folder_tree = []
    extension_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "size": 0}))
    
    total_folders = 0
    total_files = 0
    total_size = 0
    max_found_depth = 0
    all_extensions = set()
    
    root_path = os.path.abspath(root_path)
    if not os.path.exists(root_path):
        print(f"❌ 경로가 존재하지 않습니다: {root_path}")
        return [], {}, {}
    
    size_msg = "(크기 계산 포함)" if calculate_size else "(크기 계산 제외)"
    print(f"📂 폴더 구조 빠른 스캔 중: {root_path} {size_msg}")
    root_depth = root_path.rstrip('/').count('/')
    
    for dirpath, dirnames, filenames in tqdm(os.walk(root_path), desc="  스캔 중", disable=not show_progress):
        current_depth = dirpath.rstrip('/').count('/') - root_depth
        
        if current_depth > max_depth:
            dirnames.clear()
            continue
        
        max_found_depth = max(max_found_depth, current_depth)
        total_folders += 1
        
        parent_path = os.path.dirname(dirpath) if dirpath != root_path else None
        name = os.path.basename(dirpath) or dirpath
        
        file_count = len(filenames)
        total_files += file_count
        folder_size = 0
        
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower() or "(no ext)"
            all_extensions.add(ext)
            
            file_size = 0
            if calculate_size:
                try:
                    file_path = os.path.join(dirpath, filename)
                    file_size = os.path.getsize(file_path)
                    folder_size += file_size
                    total_size += file_size
                except (OSError, PermissionError):
                    pass
            
            extension_stats[dirpath][ext]["count"] += 1
            extension_stats[dirpath][ext]["size"] += file_size
        
        folder_tree.append({
            "path": dirpath,
            "parent_path": parent_path,
            "name": name,
            "depth": current_depth,
            "node_type": "folder",
            "size_bytes": folder_size,
            "file_count": file_count,
            "folder_count": len(dirnames),
            "total_size_bytes": folder_size,
            "total_file_count": file_count
        })
    
    # 하위 폴더 크기 합산 (bottom-up)
    if calculate_size and folder_tree:
        sorted_by_depth = sorted(folder_tree, key=lambda x: -x["depth"])
        path_to_item = {item["path"]: item for item in folder_tree}
        
        for item in sorted_by_depth:
            parent = item["parent_path"]
            if parent and parent in path_to_item:
                path_to_item[parent]["total_size_bytes"] += item["total_size_bytes"]
                path_to_item[parent]["total_file_count"] += item["total_file_count"]
    
    summary = {
        "total_folders": total_folders,
        "total_files": total_files,
        "total_size": total_size,
        "max_depth": max_found_depth,
        "unique_extensions": len(all_extensions)
    }
    
    return folder_tree, dict(extension_stats), summary


def insert_folder_tree(
    conn,
    scan_time: datetime,
    root_path: str,
    folder_tree: List[Dict],
    extension_stats: Dict,
    summary: Dict
):
    """폴더 트리 데이터를 PostgreSQL에 삽입"""
    
    folder_tree = calculate_tree_prefixes(folder_tree)
    
    with conn.cursor() as cur:
        if folder_tree:
            tree_rows = [
                (
                    scan_time,
                    item["path"],
                    item["parent_path"],
                    item["name"],
                    item["depth"],
                    item["node_type"],
                    item["size_bytes"],
                    item["file_count"],
                    item["folder_count"],
                    item["total_size_bytes"],
                    item["total_file_count"],
                    item.get("tree_prefix", ""),
                    item.get("sort_key", item["path"])
                )
                for item in folder_tree
            ]
            
            execute_values(cur, """
                INSERT INTO nas_folder_tree 
                (scan_time, path, parent_path, name, depth, node_type, size_bytes, 
                 file_count, folder_count, total_size_bytes, total_file_count,
                 tree_prefix, sort_key)
                VALUES %s
            """, tree_rows)
        
        ext_rows = []
        for folder_path, extensions in extension_stats.items():
            for ext, stats in extensions.items():
                ext_rows.append((scan_time, folder_path, ext, stats["count"], stats["size"]))
        
        if ext_rows:
            execute_values(cur, """
                INSERT INTO nas_folder_extensions 
                (scan_time, folder_path, extension, file_count, total_size_bytes)
                VALUES %s
            """, ext_rows)
        
        cur.execute("""
            INSERT INTO nas_folder_summary 
            (scan_time, root_path, total_folders, total_files, total_size_bytes, 
             total_size_gb, max_depth, unique_extensions)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            scan_time,
            root_path,
            summary["total_folders"],
            summary["total_files"],
            summary["total_size"],
            summary["total_size"] / (1024**3),
            summary["max_depth"],
            summary["unique_extensions"]
        ))
        
        timeseries_rows = [
            (scan_time, "nas_total_folders", summary["total_folders"], f'{{"root": "{root_path}"}}'),
            (scan_time, "nas_total_files", summary["total_files"], f'{{"root": "{root_path}"}}'),
            (scan_time, "nas_total_size_gb", summary["total_size"] / (1024**3), f'{{"root": "{root_path}"}}'),
        ]
        
        execute_values(cur, """
            INSERT INTO nas_timeseries (time, metric_name, metric_value, labels)
            VALUES %s
        """, timeseries_rows)
    
    conn.commit()
    print(f"✅ PostgreSQL에 저장 완료 ({len(folder_tree)}개 폴더)")


def format_size(size_bytes: int) -> str:
    """바이트를 읽기 쉬운 형식으로 변환"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def print_summary(summary: Dict, extension_stats: Dict):
    """통계 요약 출력"""
    print("\n" + "=" * 60)
    print("📊 NAS 폴더 스캔 요약")
    print("=" * 60)
    print(f"  총 폴더 수: {summary['total_folders']:,}개")
    print(f"  총 파일 수: {summary['total_files']:,}개")
    print(f"  총 용량: {format_size(summary['total_size'])}")
    print(f"  최대 깊이: {summary['max_depth']}")
    print(f"  확장자 종류: {summary['unique_extensions']}개")
    
    ext_totals = defaultdict(lambda: {"count": 0, "size": 0})
    for folder_path, extensions in extension_stats.items():
        for ext, stats in extensions.items():
            ext_totals[ext]["count"] += stats["count"]
            ext_totals[ext]["size"] += stats["size"]
    
    print("\n📄 확장자별 분포 (Top 10):")
    for ext, stats in sorted(ext_totals.items(), key=lambda x: -x[1]["count"])[:10]:
        print(f"  {ext:<15} {stats['count']:>10,}개  {format_size(stats['size']):>12}")


def expand_glob_patterns(patterns: List[str]) -> List[str]:
    """glob 패턴을 확장하여 실제 폴더 경로 목록 반환"""
    expanded_paths = []
    for pattern in patterns:
        if '*' in pattern or '?' in pattern:
            matches = sorted(glob.glob(pattern))
            matches = [m for m in matches if os.path.isdir(m)]
            expanded_paths.extend(matches)
        else:
            if os.path.isdir(pattern):
                expanded_paths.append(pattern)
    return expanded_paths


def main():
    parser = argparse.ArgumentParser(description="NAS 폴더 트리를 PostgreSQL에 저장")
    parser.add_argument("--path", type=str, nargs="*", default=None, help="스캔할 NAS 경로")
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--postgres-host", type=str, default=DEFAULT_PG_HOST)
    parser.add_argument("--postgres-port", type=int, default=DEFAULT_PG_PORT)
    parser.add_argument("--postgres-db", type=str, default=DEFAULT_PG_DB)
    parser.add_argument("--postgres-user", type=str, default=DEFAULT_PG_USER)
    parser.add_argument("--postgres-password", type=str, default=DEFAULT_PG_PASSWORD)
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 미리보기만")
    parser.add_argument("--keep-history", action="store_true", help="이전 스캔 데이터 유지")
    parser.add_argument("--no-progress", action="store_true", help="진행률 표시 안함")
    parser.add_argument("--fast", action="store_true", help="빠른 스캔 모드 (os.walk)")
    parser.add_argument("--no-size", action="store_true", help="파일 크기 계산 건너뛰기")
    
    args = parser.parse_args()
    
    path_patterns = args.path if args.path else DEFAULT_NAS_PATHS
    
    print("=" * 60)
    print("🌳 NAS Folder Tree Scanner for Grafana")
    print("=" * 60)
    print("📂 스캔 패턴:")
    for p in path_patterns:
        print(f"   - {p}")
    print(f"📏 최대 깊이: {args.max_depth}")
    print(f"⚡ Fast Mode: {'Yes' if args.fast else 'No'}")
    print(f"📏 Calculate Size: {'No' if args.no_size else 'Yes'}")
    print("-" * 60)
    
    scan_paths = expand_glob_patterns(path_patterns)
    
    if not scan_paths:
        print("⚠️ 매칭되는 폴더가 없습니다.")
        return
    
    print(f"\n📁 스캔할 폴더 ({len(scan_paths)}개):")
    for p in scan_paths:
        print(f"   ✓ {p}")
    
    all_folder_trees = []
    all_extension_stats = []
    all_summaries = []
    
    for i, scan_path in enumerate(scan_paths, 1):
        print(f"\n[{i}/{len(scan_paths)}] 📂 스캔 중: {scan_path}")
        
        folder_tree, extension_stats, summary = scan_folder_tree_fast(
            scan_path,
            max_depth=args.max_depth,
            show_progress=not args.no_progress,
            calculate_size=not args.no_size
        )
        
        if folder_tree:
            all_folder_trees.append(folder_tree)
            all_extension_stats.append(extension_stats)
            all_summaries.append(summary)
            print(f"   ✅ {summary['total_folders']}개 폴더, {summary['total_files']}개 파일, {format_size(summary['total_size'])}")
    
    if not all_folder_trees:
        print("⚠️ 스캔할 폴더가 없습니다.")
        return
    
    # 전체 요약
    merged_summary = {
        "total_folders": sum(s["total_folders"] for s in all_summaries),
        "total_files": sum(s["total_files"] for s in all_summaries),
        "total_size": sum(s["total_size"] for s in all_summaries),
        "max_depth": max(s["max_depth"] for s in all_summaries),
        "unique_extensions": len(set().union(*[
            set(ext for folder in ext_stats.values() for ext in folder.keys())
            for ext_stats in all_extension_stats
        ]))
    }
    
    merged_ext_stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "size": 0}))
    for ext_stats in all_extension_stats:
        for folder_path, extensions in ext_stats.items():
            for ext, stats in extensions.items():
                merged_ext_stats[folder_path][ext]["count"] += stats["count"]
                merged_ext_stats[folder_path][ext]["size"] += stats["size"]
    
    print_summary(merged_summary, dict(merged_ext_stats))
    
    if args.dry_run:
        print("\n✅ Dry run 완료 (DB 저장 안함)")
        return
    
    print("\n💾 PostgreSQL에 저장 중...")
    try:
        conn = get_postgres_connection(
            args.postgres_host,
            args.postgres_port,
            args.postgres_db,
            args.postgres_user,
            args.postgres_password
        )
        
        ensure_schema(conn)
        
        if not args.keep_history:
            with conn.cursor() as cur:
                for scan_path in scan_paths:
                    cur.execute("DELETE FROM nas_folder_tree WHERE path LIKE %s", (f"{scan_path}%",))
                    cur.execute("DELETE FROM nas_folder_extensions WHERE folder_path LIKE %s", (f"{scan_path}%",))
                    cur.execute("DELETE FROM nas_folder_summary WHERE root_path = %s", (scan_path,))
            conn.commit()
            print("🗑️  이전 스캔 데이터 삭제 완료")
        
        scan_time = datetime.now()
        
        for folder_tree, extension_stats, summary, scan_path in zip(
            all_folder_trees, all_extension_stats, all_summaries, scan_paths
        ):
            insert_folder_tree(conn, scan_time, scan_path, folder_tree, extension_stats, summary)
        
        conn.close()
        print("\n🎉 완료!")
        
    except Exception as e:
        print(f"\n❌ PostgreSQL 연결 실패: {e}")
        raise


if __name__ == "__main__":
    main()
