FROM alpine:3.20

# pg_dump (postgresql15-client) + restic + busybox cron + tini (PID1) + bash + tz
# 2026-05-28: dcron 은 PID1 일 때 setpgid:EPERM 으로 즉시 종료 → restart-loop 발생.
# busybox crond 는 EPERM 무시하지만 fork 직후 어쩌다 같은 메시지 출력. tini 를 PID1 으로 두고
# crond -f 를 자식으로 돌리면 signal/reap 도 정상 동작 — 컨테이너 cron 표준 패턴.
RUN apk add --no-cache \
        postgresql15-client \
        restic \
        busybox-suid \
        tini \
        tzdata \
        bash \
    && ln -sf /usr/share/zoneinfo/Asia/Seoul /etc/localtime \
    && echo "Asia/Seoul" > /etc/timezone

COPY backup.sh /usr/local/bin/backup.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY crontab /etc/crontabs/root

RUN chmod +x /usr/local/bin/backup.sh /usr/local/bin/entrypoint.sh \
    && mkdir -p /var/log

# tini = PID1 → reaps zombies + 신호 전달. crond 는 entrypoint 가 exec 로 전환.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
