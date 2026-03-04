"""
MinIO/S3 클라이언트 공유 유틸리티

여러 업로더 스크립트에서 공통으로 사용하는 S3 클라이언트 생성 및 버킷 관리 로직.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from .config import MinioSettings


def get_s3_client(
    endpoint: str,
    access_key: str,
    secret_key: str,
    signature_version: str = "s3v4",
    addressing_style: str = "path",
):
    """
    S3 호환 클라이언트 생성.

    Args:
        endpoint: MinIO/S3 엔드포인트 URL
        access_key: 액세스 키
        secret_key: 시크릿 키
        signature_version: 서명 버전 (기본: s3v4)
        addressing_style: 주소 스타일 (기본: path)

    Returns:
        boto3 S3 클라이언트
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version=signature_version,
            s3={"addressing_style": addressing_style},
        ),
    )


def get_s3_client_from_settings(settings: "MinioSettings"):
    """
    MinioSettings 객체로부터 S3 클라이언트 생성.

    Args:
        settings: MinioSettings 인스턴스

    Returns:
        boto3 S3 클라이언트
    """
    return get_s3_client(
        endpoint=settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
    )


def create_bucket_if_not_exists(s3_client, bucket_name: str) -> bool:
    """
    버킷이 없으면 생성.

    Args:
        s3_client: boto3 S3 클라이언트
        bucket_name: 버킷 이름

    Returns:
        True if created, False if already exists
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return False
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
        return True


def resolve_bucket_for_path(
    source_path: str,
    default_bucket: str,
    organized_bucket: str = "raw-data",
    processed_bucket: str = "image-data",
) -> str:
    """
    경로 규칙에 따라 버킷 선택.

    Args:
        source_path: 소스 파일 경로
        default_bucket: 기본 버킷
        organized_bucket: organized_videos 경로용 버킷
        processed_bucket: processed_data 경로용 버킷

    Returns:
        선택된 버킷 이름
    """
    parts = [p.lower() for p in Path(source_path).parts]
    if "organized_videos" in parts:
        return organized_bucket
    if "processed_data" in parts:
        return processed_bucket
    return default_bucket


__all__ = [
    "get_s3_client",
    "get_s3_client_from_settings",
    "create_bucket_if_not_exists",
    "resolve_bucket_for_path",
]
