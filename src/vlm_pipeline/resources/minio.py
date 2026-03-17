"""MinIOResource — Dagster ConfigurableResource, S3 호환 클라이언트."""

from __future__ import annotations

import os
from functools import cached_property
from io import BytesIO
from threading import Lock
from typing import BinaryIO
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from dagster import ConfigurableResource


class MinIOResource(ConfigurableResource):
    """MinIO/S3 호환 리소스."""

    endpoint: str  # "http://minio:9000" (Docker 내부)
    access_key: str
    secret_key: str
    multipart_chunk_mb: int = 8
    upload_max_concurrency: int = 4

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        endpoint = (endpoint or "").strip()
        if not endpoint:
            return endpoint
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"http://{endpoint}"

    @cached_property
    def client(self):
        """boto3 S3 클라이언트 생성 (cached)."""
        return boto3.client(
            "s3",
            endpoint_url=self._normalize_endpoint(self.endpoint),
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )

    @cached_property
    def transfer_config(self) -> TransferConfig:
        """멀티파트 업로드 설정 (upload_fileobj용)."""
        chunk_raw = os.getenv("MINIO_MULTIPART_CHUNK_MB", str(self.multipart_chunk_mb or 8))
        concurrency_raw = os.getenv(
            "MINIO_UPLOAD_MAX_CONCURRENCY",
            str(self.upload_max_concurrency or 4),
        )
        try:
            chunk_mb = int(chunk_raw)
        except ValueError:
            chunk_mb = int(self.multipart_chunk_mb or 8)
        try:
            max_concurrency = int(concurrency_raw)
        except ValueError:
            max_concurrency = int(self.upload_max_concurrency or 4)

        chunk_mb = max(5, chunk_mb)
        chunk_bytes = chunk_mb * 1024 * 1024
        max_concurrency = max(1, min(16, max_concurrency))
        return TransferConfig(
            multipart_threshold=chunk_bytes,
            multipart_chunksize=chunk_bytes,
            max_concurrency=max_concurrency,
            use_threads=max_concurrency > 1,
        )

    @cached_property
    def _ensured_buckets(self) -> set[str]:
        return set()

    @cached_property
    def _ensure_bucket_lock(self) -> Lock:
        return Lock()

    def _ensure_bucket_once(self, bucket: str) -> None:
        normalized_bucket = str(bucket or "").strip()
        if not normalized_bucket:
            return
        if normalized_bucket in self._ensured_buckets:
            return

        with self._ensure_bucket_lock:
            if normalized_bucket in self._ensured_buckets:
                return
            self.ensure_bucket(normalized_bucket)
            self._ensured_buckets.add(normalized_bucket)

    def upload(
        self,
        bucket: str,
        key: str,
        data: bytes | bytearray | BinaryIO,
        content_type: str = "application/octet-stream",
    ) -> None:
        """MinIO 업로드.

        - bytes/bytearray: put_object
        - file-like object: upload_fileobj (멀티파트/스트리밍)
        """
        self._ensure_bucket_once(bucket)
        if hasattr(data, "read"):
            self.upload_fileobj(bucket, key, data, content_type=content_type)
            return

        payload = data if isinstance(data, (bytes, bytearray)) else bytes(data)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=BytesIO(payload),
            ContentLength=len(payload),
            ContentType=content_type,
        )

    def upload_fileobj(
        self,
        bucket: str,
        key: str,
        fileobj: BinaryIO,
        content_type: str = "application/octet-stream",
    ) -> None:
        """파일 객체를 스트리밍 업로드 (멀티파트)."""
        self._ensure_bucket_once(bucket)
        if hasattr(fileobj, "seek"):
            fileobj.seek(0)
        self.client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
            Config=self.transfer_config,
        )

    def upload_file(
        self,
        bucket: str,
        key: str,
        file_path: str | Path,
        content_type: str = "application/octet-stream",
    ) -> None:
        """MinIO 파일 단위 멀티파트 병렬 업로드 (1GB 이상 대용량 최적화)."""
        self._ensure_bucket_once(bucket)
        self.client.upload_file(
            Filename=str(file_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
            Config=self.transfer_config,
        )

    def download(self, bucket: str, key: str) -> bytes:
        """MinIO에서 바이트 데이터 다운로드."""
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def download_fileobj(self, bucket: str, key: str, fileobj: BinaryIO) -> None:
        """MinIO 객체를 파일 객체로 스트리밍 다운로드."""
        if hasattr(fileobj, "seek"):
            fileobj.seek(0)
        self.client.download_fileobj(Bucket=bucket, Key=key, Fileobj=fileobj)
        if hasattr(fileobj, "seek"):
            fileobj.seek(0)

    def copy(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        """MinIO 내부 S3 copy (네트워크 비용 0)."""
        self._ensure_bucket_once(dst_bucket)
        self.client.copy_object(
            Bucket=dst_bucket,
            Key=dst_key,
            CopySource={"Bucket": src_bucket, "Key": src_key},
        )

    def ensure_bucket(self, bucket: str) -> None:
        """버킷이 없으면 생성."""
        try:
            self.client.head_bucket(Bucket=bucket)
        except Exception:
            self.client.create_bucket(Bucket=bucket)

    def delete(self, bucket: str, key: str) -> None:
        """MinIO 객체 삭제."""
        self.client.delete_object(Bucket=bucket, Key=key)
