#!/usr/bin/env bash
# 좌석 라우터에 HTTPS 를 켠다. 켜기 전까지는 이 디렉토리가 비어 있고 HTTP 로만 돈다.
#
# 왜 기본이 꺼짐인가: 지금 접속은 전부 사내 LAN 의 raw IP 다. DNS 도 사내 CA 도 없어서
# 자체서명 인증서를 켜면 5명 전원이 매번 브라우저 경고를 보게 되고, 얻는 보안은 없다
# (인증이 붙는 게 아니다). FiftyOne Enterprise 는 HTTPS 종단을 요구하므로 "그때 켤 수
# 있게" 준비만 해 둔다. 사내 CA/정식 인증서가 생기면 cert.pem/key.pem 만 갈아끼우면 된다.
#
# 사용:  ./gen-seat-cert.sh [호스트명]     (기본 hostname = fiftyone.user.local)
#        docker exec docker-analysis-fiftyone-proxy-1 nginx -s reload
set -euo pipefail
cd "$(dirname "$0")/seat-tls"

HOST="${1:-fiftyone.user.local}"
IP="${SEAT_TLS_IP:-10.0.0.10}"

# DNS 레코드가 아직 없어도 쓰이도록 IP 를 SAN 에 같이 넣는다. 레코드가 생기면 호스트명
# 쪽이 그대로 유효해져서 인증서를 다시 만들 필요가 없다.
openssl req -x509 -newkey rsa:2048 -nodes -days 825 \
  -keyout key.pem -out cert.pem \
  -subj "/CN=${HOST}" \
  -addext "subjectAltName=DNS:${HOST},DNS:localhost,IP:${IP},IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" 2>/dev/null

chmod 644 cert.pem && chmod 640 key.pem

cat > tls.conf <<'CONF'
# gen-seat-cert.sh 가 생성. HTTP(:5151) 블록과 같은 좌석 라우팅을 그대로 쓴다.
server {
    listen 5443 ssl;
    http2 on;
    server_name _;

    ssl_certificate     /etc/nginx/seat-tls/cert.pem;
    ssl_certificate_key /etc/nginx/seat-tls/key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_session_cache   shared:SEAT:10m;

    resolver 127.0.0.11 valid=10s ipv6=off;
    client_max_body_size 64m;
    add_header Set-Cookie $seat_cookie always;

    location / {
        proxy_pass http://$seat_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 1d;
        proxy_send_timeout 1d;
    }
}
CONF

echo "생성 완료: $(pwd)"
openssl x509 -in cert.pem -noout -subject -enddate -ext subjectAltName
echo
echo "적용: docker exec docker-analysis-fiftyone-proxy-1 nginx -s reload"
echo "접속: https://${IP}:${SEAT_TLS_HOST_PORT:-5443}/   (자체서명 → 브라우저 경고 1회 수락)"
echo "끄기: rm -f tls.conf cert.pem key.pem && nginx -s reload"
