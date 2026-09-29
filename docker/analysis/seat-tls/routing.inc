# 좌석 라우팅 본문 — `nginx-seats.conf` 의 HTTP server 블록과 (켠다면) `seat-tls/tls.conf` 의
# HTTPS server 블록이 **둘 다 이 파일을 include** 한다. 라우팅을 한 벌만 두려는 목적이다:
# 예전엔 TLS 블록이 proxy_pass 만 갖고 있어서, HTTPS 를 켜는 순간 쿠키 없는 접속자가 전원 좌석 1 로
# 몰리는 조용한 퇴행이 생길 수 있었다(opus 리뷰 M4, 2026-09-03).
#
# 담기는 것: server 레벨 공통 지시자(absolute_redirect/resolver/바디상한/Set-Cookie) + 모든 location.
# 담기지 않는 것: `listen`/`server_name`/`ssl_*` — 프로토콜마다 달라서 각 server 블록에 남는다.
#
# ⚠️ 이 파일은 `./analysis/seat-tls` **디렉토리** 마운트 안에 있다(단일 파일 마운트가 아니라 inode 교체
#   함정이 없다 — 여기만 고쳤으면 `nginx -s reload` 로 반영된다). 파일이 없으면 nginx 가 기동하지
#   못하므로 지우지 말 것. .gitignore 대상은 cert/key/tls.conf 뿐이고 이 파일은 추적된다.

    # return 302 가 절대 URL 을 만들면 nginx 의 listen 포트(5151)가 붙는다 — 호스트에서는 :5153 이라
    # 브라우저가 엉뚱한 포트로 간다(실측). 상대경로로 내보내면 브라우저가 현재 origin 을 유지한다.
    absolute_redirect off;

    # proxy_pass 에 변수를 쓰면 nginx 가 부팅 시점이 아니라 요청 시점에 이름을 푼다 —
    # resolver 가 없으면 전부 502 다("no resolver defined", 2026-08-31 실측). Docker 내장 DNS.
    # 대신 좌석 하나가 죽어도 그 좌석만 502 이고 나머지 좌석은 멀쩡하다.
    resolver 127.0.0.11 valid=10s ipv6=off;

    # 패널 상태가 수 MB 단위로 오간다. nginx 기본 1m 이면 큰 패널 요청이 413 으로 죽는다.
    # 1g 인 이유: 오퍼레이터 모달의 zip 업로드가 base64(4/3 팽창)로 이 좌석 경로를 지나간다 —
    # APP_MODAL_UPLOAD_MAX_MB(기본 512) × 1.34 + 여유. 이 값만 올려도 모달 상한은 안 오른다
    # (실제 상한은 플러그인 상수). 더 큰 번들은 /__upload/ 쪽(20g, 스트리밍, 좌석 미경유).
    client_max_body_size 1g;

    add_header Set-Cookie $seat_cookie always;

    # 좌석 미정(쿠키·IP 표·?seat 전부 없음) → 자동 배정기(analysis-sync `/seat/assign`) 로 넘긴다.
    # 배정기는 각 좌석의 점유 카운터(fiftyone_relaunch.py :5160, 브라우저 연결 수)를 보고 빈 좌석을
    # 골라 302 `<원래URI>?seat=N` 으로 돌려보낸다 → 그 요청이 위 map 을 타며 쿠키가 박힌다.
    # 사람은 아무것도 고르지 않는다. 배정기가 죽어 있으면(502/504) 정적 선택 페이지로 폴백 —
    # 자동이 안 되는 순간에도 조용히 좌석 1 을 공유하는 옛 경로로는 돌아가지 않는다.
    location = /__seat_assign {
        set $assign_upstream analysis-sync:8010;   # 변수로 둬야 부팅 시점이 아니라 요청 시점에 DNS 를 푼다
        proxy_pass http://$assign_upstream/seat/assign;
        proxy_set_header X-Original-URI $request_uri;   # rewrite 뒤에도 클라이언트 원래 URI 가 남는 변수
        proxy_set_header X-Real-IP       $remote_addr;
        proxy_connect_timeout 2s;
        proxy_read_timeout    5s;
        proxy_intercept_errors on;
        error_page 502 503 504 = @seat_fallback;
    }

    # 브라우저 업로드 페이지·API (analysis-sync `/upload/*`) — 좌석 라우팅 밖(prefix 매칭이 `/` 보다 우선).
    # 번들 zip 을 raw body 로 PUT 하므로 바디 상한을 크게, 버퍼링 없이 업스트림으로 흘린다(nginx 임시파일 없음).
    # proxy_pass 에 변수를 쓰면 location 접두 치환이 안 되므로 rewrite 로 /__upload/ → /upload/ 를 직접 바꾼다.
    location /__upload/ {
        set $assign_upstream analysis-sync:8010;
        rewrite ^/__upload/(.*)$ /upload/$1 break;
        proxy_pass http://$assign_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host       $host;
        proxy_set_header X-Real-IP  $remote_addr;
        client_max_body_size 20g;
        proxy_request_buffering off;
        proxy_read_timeout  1h;
        proxy_send_timeout  1h;
    }

    location @seat_fallback {
        default_type text/html;
        return 200 '<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>FiftyOne 좌석 선택</title><style>body{font-family:sans-serif;max-width:640px;margin:48px auto;padding:0 16px;color:#222}a.seat{display:block;padding:14px 18px;margin:10px 0;border:1px solid #888;border-radius:8px;text-decoration:none;color:#111;font-size:18px}a.seat:hover{background:#f0f4ff}p.note{color:#555;font-size:14px}</style></head><body><h1>FiftyOne 좌석 선택 (자동 배정기 응답 없음)</h1><p>평소엔 빈 좌석이 자동 배정됩니다. 지금은 배정기(analysis-sync)가 응답하지 않아 직접 고릅니다 — 다른 사람이 쓰지 않는 좌석을 선택하세요. 선택은 이 브라우저에 1년간 기억됩니다.</p><a class="seat" href="/?seat=1">좌석 1 — 무거운 프롬프트 분석용 (메모리 상한 없음)</a><a class="seat" href="/?seat=2">좌석 2</a><a class="seat" href="/?seat=3">좌석 3</a><p class="note">좌석을 바꾸려면 주소 뒤에 <code>?seat=N</code> 을 붙여 한 번 접속하면 됩니다.</p></body></html>';
    }

    # 쿠키가 가리키는 좌석이 죽었거나 기동 중이면(502/503/504) 배정기로 돌려보내 다른 좌석을 받는다 —
    # 이게 없으면 유효한 쿠키 때문에 배정기를 다시 안 타서 1년간 맨 502 만 본다.
    # 배정기는 그 좌석의 카운터가 죽어 있거나 503(앱 미준비)이면 후보에서 빼므로 같은 좌석으로 돌아가지 않는다.
    location @seat_dead {
        default_type application/json;
        if ($is_nav = 1) {
            return 302 /__seat_assign;
        }
        # API/SSE 요청: HTML 로 보내지 않는다 — 앱이 오류로 처리하고, 사용자가 새로고침하면 navigation 이
        # 배정기를 타 새 좌석을 받는다.
        return 503 '{"error":"seat_unavailable","hint":"이 좌석(FiftyOne 프로세스)이 응답하지 않습니다 — 페이지를 새로고침하면 다른 좌석으로 자동 배정됩니다"}';
    }

    location / {
        default_type application/json;
        set $route "$seat_ok$is_nav";
        if ($route = "01") {       # 좌석 미정 + 화면 이동 → 자동 배정기
            rewrite ^ /__seat_assign last;
        }
        if ($route = "00") {       # 좌석 미정 + API/SSE → JSON 오류 (HTML 금지)
            return 409 '{"error":"seat_unassigned","hint":"좌석이 배정되지 않은 요청입니다 — 페이지를 새로고침하세요"}';
        }
        proxy_intercept_errors on;
        error_page 502 503 504 = @seat_dead;
        # ⚠️ add_header 는 이 레벨에 하나라도 있으면 server 레벨 것을 상속하지 않는다 — Set-Cookie 를 여기
        #   다시 쓰지 않으면 좌석 쿠키가 영영 안 박힌다(X-Seat 추가 때 잡음).
        add_header Set-Cookie $seat_cookie always;
        add_header X-Seat $seat_key always;   # 디버깅: 이 응답이 어느 좌석에서 왔나 (브라우저 devtools/ curl -I)
        proxy_pass http://$seat_upstream;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        $connection_upgrade;

        # /events 는 SSE(sse_starlette) 다. 버퍼링을 끄지 않으면 이벤트가 프록시에 고여
        # App 이 로딩에서 멈춘 것처럼 보인다 — 이 두 줄이 이 설정의 핵심이다.
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 1d;
        proxy_send_timeout 1d;
    }
