# 내부용 커스텀 Label Studio (labelstudio-internal)
#
# 베이스 = 순정 v1.23.0 (rev 2a9bfbc), digest 핀 — 교체 전 내부 운영본과 동일 바이트.
# 오버레이 = 외주(GCP) 포크의 변경 파일 전량. 기준 스냅샷 = GCP VM 실배포본
#   (-c4 기준: 2026-07-24T01:30Z 빌드, VM image 31f14c23ed6d — 단축키 n 키 추가).
#   기능 목록·마이그레이션·리스크: docs/design-docs/gcpls/gcpls-adoption-inventory.md
# 외주용 제한(labeler 기본거부 등)은 전부 역할(role) 기반 — 내부 사용자는 role=admin으로 운영
#   (docs/runbook/labelstudio-ops.md "역할 운영" 참고).
#
# FE(web/dist)는 빌드 산출물이라 검증된 배포 이미지 스테이지에서 복사.
# ponytail: FE 소스 빌드 체인 없음 — 포크 재개발/업그레이드 결정 시 독립 포크 레포로 승격(런북 "출구 경로").
FROM gcpls-deployed:20260724b AS gcpls

FROM heartexlabs/label-studio@sha256:aa461572e8f9d86a1bf9520c1db620204e86160fd2f80dd7e9d40ac84a8828ea

COPY --from=gcpls --chown=1001:0 /label-studio/web/dist /label-studio/web/dist
COPY --chown=1001:0 overlay/ /label-studio/label_studio/

# 배포본 빌드와 동일한 collectstatic 단계 (커스텀 FE 정적 파일 매니페스트 재생성)
USER root
RUN cd /label-studio && (python3 label_studio/manage.py collectstatic --no-input 2>&1 | tail -3 || echo "collectstatic skipped")
USER 1001
