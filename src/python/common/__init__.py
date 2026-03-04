# Common package for shared utilities (config, db helpers, logging)
#
# 공유 모듈:
# - config.py: 설정 로더 (Pydantic BaseSettings)
# - db.py: DuckDB 연결 및 스키마 관리
# - minio_client.py: MinIO/S3 클라이언트 유틸리티
# - motherduck.py: MotherDuck 연결 유틸리티
# - path_utils.py: 경로 변환 및 정규화 유틸리티
# - sanitizer.py / validator.py / checksum.py: INGEST 전처리 유틸리티
# - file_loader.py / video_loader.py: 1-pass 미디어 로더
# - phash.py: 이미지 유사도 해시 유틸리티