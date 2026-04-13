"""Staging → Production MinIO 선택적 동기화.

LS 연동을 위해 staging MinIO(:9002)의 클립·라벨 JSON을
production MinIO(:9000)로 복사한다.

환경변수:
    MINIO_ENDPOINT      현재 환경의 MinIO (staging일 때 :9002)
    LS_MINIO_ENDPOINT   LS가 바라보는 production MinIO (기본 :9000)
    MINIO_ACCESS_KEY    공통 access key
    MINIO_SECRET_KEY    공통 secret key
"""

from __future__ import annotations

import os
from typing import Callable

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

_DEFAULT_PRODUCTION_ENDPOINT = "http://172.168.47.36:9000"

_SYNC_TARGETS = [
    ("vlm-processed", "{folder}/clips/"),
    ("vlm-labels", "{folder}/events/"),
]


def _build_s3_client(endpoint: str) -> "boto3.client":
    ak = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    sk = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ls_minio_endpoint() -> str:
    """LS가 바라보는 production MinIO endpoint."""
    return os.environ.get("LS_MINIO_ENDPOINT", _DEFAULT_PRODUCTION_ENDPOINT)


def is_cross_sync_needed() -> bool:
    """현재 환경의 MinIO와 LS용 MinIO가 다르면 True."""
    current = os.environ.get("MINIO_ENDPOINT", "").rstrip("/")
    target = ls_minio_endpoint().rstrip("/")
    return bool(current) and current != target


def sync_folder_for_ls(
    folder_name: str,
    *,
    source_endpoint: str | None = None,
    target_endpoint: str | None = None,
    log_fn: Callable[[str], None] | None = None,
) -> int:
    """staging MinIO → production MinIO로 LS 관련 객체를 복사한다.

    복사 대상:
        vlm-processed/{folder_name}/clips/*
        vlm-labels/{folder_name}/events/*

    이미 대상에 같은 key가 있으면 건너뛴다.

    Returns:
        복사된 객체 수
    """
    src_ep = source_endpoint or os.environ.get("MINIO_ENDPOINT", "")
    dst_ep = target_endpoint or ls_minio_endpoint()

    if not src_ep:
        if log_fn:
            log_fn("MINIO_ENDPOINT 미설정 — 동기화 생략")
        return 0

    if src_ep.rstrip("/") == dst_ep.rstrip("/"):
        if log_fn:
            log_fn("source와 target MinIO가 동일 — 동기화 생략")
        return 0

    src = _build_s3_client(src_ep)
    dst = _build_s3_client(dst_ep)

    copied = 0
    for bucket, prefix_tmpl in _SYNC_TARGETS:
        prefix = prefix_tmpl.format(folder=folder_name)
        try:
            paginator = src.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if _exists_in_target(dst, bucket, key):
                        continue
                    try:
                        resp = src.get_object(Bucket=bucket, Key=key)
                        body = resp["Body"].read()
                        ct = resp.get("ContentType", "application/octet-stream")
                        dst.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ct)
                        copied += 1
                        if log_fn:
                            log_fn(f"복사: {bucket}/{key}")
                    except Exception as exc:
                        if log_fn:
                            log_fn(f"복사 실패: {bucket}/{key} — {exc}")
        except Exception as exc:
            if log_fn:
                log_fn(f"목록 조회 실패 ({bucket}/{prefix}): {exc}")

    if log_fn:
        log_fn(f"staging→production 동기화 완료: {copied}건")
    return copied


def _exists_in_target(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
