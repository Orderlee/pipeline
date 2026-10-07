---
name: daily_worklog
description: 당일 Git·미커밋 변경·컨테이너 상태를 WORKLOG.md와 CLAUDE2.md에 기록할 때 사용
---

# Daily worklog

스크립트는 평일만 처리하고 같은 날짜 항목은 건너뛴다. Git·Docker가 없으면 가능한 정보만 기록한다.

```bash
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --dry-run
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --date YYYY-MM-DD
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --worklog-only
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --claude-only
```

- 실제 기록은 `WORKLOG.md`, `CLAUDE2.md`를 수정한다. 먼저 `--dry-run`으로 결과를 확인한다.
- cron wrapper는 `.agent/skill/daily_worklog/scripts/daily_worklog_cron.sh`이며 등록 여부는 `crontab -l | grep daily_worklog`로 확인한다.
- cron 등록·해제는 사용자 권한과 의도가 필요한 외부 상태 변경이므로 명시 승인 후에만 한다.
