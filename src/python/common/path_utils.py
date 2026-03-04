"""
경로 유틸리티 공유 모듈

로컬/Docker 환경 간 경로 변환, NAS 경로 정규화 등 공통 로직.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import List, Optional


# 기본 경로 매핑 (로컬 <-> Docker)
DEFAULT_PATH_MAPPINGS = [
    # (source_prefix, target_prefix)
    ("/home/pia/mou/nas_192tb/datasets", "/nas/datasets"),
    ("/mnt/nas/datasets", "/nas/datasets"),
    ("/nas/datasets", "/home/pia/mou/nas_192tb/datasets"),
]


def resolve_local_path(
    source_path: str,
    prefix_from: Optional[str] = None,
    prefix_to: Optional[str] = None,
    additional_mappings: Optional[List[tuple]] = None,
) -> str:
    """
    실행 환경별 경로 prefix 차이를 보정해 로컬 접근 가능한 경로로 변환.

    Args:
        source_path: 원본 경로
        prefix_from: 변환할 prefix들 (세미콜론 구분)
        prefix_to: 변환 대상 prefix
        additional_mappings: 추가 매핑 리스트 [(from, to), ...]

    Returns:
        로컬에서 접근 가능한 경로
    """
    if os.path.exists(source_path):
        return source_path

    candidates: List[str] = []

    # 명시적 prefix 변환
    if prefix_from and prefix_to:
        prefixes = [p.strip() for p in prefix_from.split(";") if p.strip()]
        for pf in prefixes:
            if source_path.startswith(pf):
                candidates.append(prefix_to.rstrip("/") + source_path[len(pf):])

    # 기본 매핑 규칙 적용
    for src_prefix, dst_prefix in DEFAULT_PATH_MAPPINGS:
        if source_path.startswith(src_prefix):
            candidates.append(dst_prefix + source_path[len(src_prefix):])

    # 추가 매핑 적용
    if additional_mappings:
        for src_prefix, dst_prefix in additional_mappings:
            if source_path.startswith(src_prefix):
                candidates.append(dst_prefix + source_path[len(src_prefix):])

    # 존재하는 첫 번째 경로 반환
    for c in candidates:
        if os.path.exists(c):
            return c

    return source_path


def normalize_source_path(path: str) -> str:
    """
    소스 경로 정규화 (중복 슬래시 제거, 상대경로 해결).

    Args:
        path: 정규화할 경로

    Returns:
        정규화된 경로
    """
    # 중복 슬래시 제거
    while "//" in path:
        path = path.replace("//", "/")

    # 후행 슬래시 제거 (루트 제외)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return path


def build_object_key(
    source_path: str,
    asset_id: str,
    nas_root: Optional[str] = None,
    mode: str = "relative",
    prefix: str = "",
) -> str:
    """
    MinIO 오브젝트 키 생성.

    Args:
        source_path: 소스 파일 경로
        asset_id: 에셋 ID
        nas_root: NAS 루트 경로 (relative 모드용)
        mode: 키 생성 모드 (relative, basename, asset_id)
        prefix: 키 prefix

    Returns:
        오브젝트 키
    """
    key = ""

    if mode == "relative" and nas_root:
        try:
            rel = Path(source_path).resolve().relative_to(Path(nas_root).resolve())
            key = str(PurePosixPath(rel))
        except Exception:
            key = Path(source_path).name
    elif mode == "asset_id":
        key = asset_id
    else:
        key = Path(source_path).name

    if prefix:
        key = str(PurePosixPath(prefix) / key)

    return key.lstrip("/")


def get_relative_path(path: str, base: str) -> str:
    """
    기준 경로 대비 상대 경로 계산.

    Args:
        path: 대상 경로
        base: 기준 경로

    Returns:
        상대 경로 문자열
    """
    try:
        return str(Path(path).resolve().relative_to(Path(base).resolve()))
    except ValueError:
        return Path(path).name


__all__ = [
    "DEFAULT_PATH_MAPPINGS",
    "resolve_local_path",
    "normalize_source_path",
    "build_object_key",
    "get_relative_path",
]
