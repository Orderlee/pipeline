import os
import sys
import json
from pathlib import Path

# Add src to pythonpath
sys.path.insert(0, os.path.abspath("src"))

from vlm_pipeline.lib.env_utils import IS_STAGING
from vlm_pipeline.resources.config import PipelineConfig
from vlm_pipeline.resources.duckdb import DuckDBResource
from vlm_pipeline.resources.minio import MinIOResource

from vlm_pipeline.defs.ingest.ops import register_incoming, normalize_and_archive

def main():
    print("Testing raw_ingest...")
    os.environ["DAGSTER_HOME"] = "/app/dagster_home_staging"
    os.environ["MINIO_ENDPOINT"] = "http://172.168.47.36:9003"
    os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["MINIO_SECRET_KEY"] = "minioadmin"
    
    db = DuckDBResource(db_path="docker/data/staging.duckdb")
    minio = MinIOResource(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"]
    )
    
    config = PipelineConfig()
    print("Config archive_pending_dir:", config.archive_pending_dir)
    
    manifest_path = "/nas/staging/incoming/.manifests/pending/auto_bootstrap_20260313_045954_612810_001_001.json"
    print(f"Loading manifest: {manifest_path}")
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    
    # 1. Register
    class DummyContext:
        class Log:
            def warning(self, msg): print("WARN:", msg)
            def error(self, msg): print("ERR:", msg)
            def info(self, msg): print("INFO:", msg)
            def debug(self, msg): print("DEBUG:", msg)
        log = Log()
        
    ctx = DummyContext()
    rejections = []
    
    print("Running register_incoming...")
    records = register_incoming(ctx, db, manifest, ingest_rejections=rejections)
    print(f"Registered {len(records)} records")
    
    # 2. Normalize and archive
    print("Running normalize_and_archive... (This is where it likely hangs)")
    target_archive_dir = "/home/pia/mou/staging/archive_pending"
    retry_candidates = []
    uploaded = normalize_and_archive(
        ctx, db, minio, records, target_archive_dir,
        ingest_rejections=rejections,
        retry_candidates=retry_candidates
    )
    print(f"Uploaded {len(uploaded)} records")

if __name__ == "__main__":
    main()
