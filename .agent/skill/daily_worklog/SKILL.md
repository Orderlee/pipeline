---
name: daily_worklog
description: Skill that automatically collects daily work activity and records it in WORKLOG.md and CLAUDE2.md every weekday at 5:30 PM
---

# Daily Worklog Auto-Recording Skill

Runs automatically via crontab every weekday at 5:30 PM, collecting the day's work activity and recording it in `WORKLOG.md` and `CLAUDE2.md`.

## 📋 Core Features

### Items Collected
- **Git commits**: commit messages, authors, and changed files across all branches for the day
- **Changed file classification**: categorized by pipeline code / infrastructure & config / scripts / tests / documentation
- **Problem and fix detection**: automatically detects keywords like `fix`, `bug`, `복구`, `해결` in commit messages
- **IDE detection**: automatically detects whether **VSCode**, **Cursor**, or **Antigravity** was used that day
- **Docker status**: records the status of pipeline service containers
- **Uncommitted changes**: also detects file modifications outside of git

### Recording Targets
| File | Content |
|------|---------|
| `WORKLOG.md` | Detailed work history + problems/solutions + change statistics + service status |
| `CLAUDE2.md` | Troubleshooting runbook entries + key file changes + fix commit summaries |

### Safety Measures
- **Duplicate prevention**: skips if an entry for that date already exists
- **Weekend skip**: automatically skips execution on Saturdays and Sundays
- **Log retention**: execution logs stored in `.worklog_logs/` (auto-purged after 30 days)

## 🛠 Usage

### 1. Automatic Execution (crontab)
Already registered:
```
30 17 * * 1-5  /home/user/work_p/Datapipeline-Data-data_pipeline/.agent/skill/daily_worklog/scripts/daily_worklog_cron.sh
```

### 2. Manual Execution
```bash
cd /home/user/work_p/Datapipeline-Data-data_pipeline

# Run for today
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py

# Dry run (no file modifications)
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --dry-run

# Run for a specific date
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --date 2026-03-16

# Update WORKLOG.md only
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --worklog-only

# Update CLAUDE2.md only
python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --claude-only
```

### 3. Run via Cron Wrapper (with Logging)
```bash
bash .agent/skill/daily_worklog/scripts/daily_worklog_cron.sh
bash .agent/skill/daily_worklog/scripts/daily_worklog_cron.sh --dry-run
```

## 📁 File Structure
```
.agent/skill/daily_worklog/
├── SKILL.md                          # This document
└── scripts/
    ├── daily_worklog.py              # Main Python script
    └── daily_worklog_cron.sh         # Crontab wrapper (PATH/log setup)
```

## ⚙️ Crontab Management

### Check Current Registration
```bash
crontab -l | grep daily_worklog
```

### Temporarily Disable
```bash
crontab -l | sed 's|^30 17|#30 17|' | crontab -
```

### Re-enable
```bash
crontab -l | sed 's|^#30 17|30 17|' | crontab -
```

### Remove Completely
```bash
crontab -l | grep -v daily_worklog | crontab -
```

## ⚠️ Caveats
- **git required**: `git log` must work from the repository root.
- **docker optional**: If Docker is unavailable, the service status section will simply be empty — it is not an error.
- **IDE detection limitations**: IDE detection is based on file modification timestamps, so if an IDE is opened without modifying any files, it may not be detected.
- **WORKLOG.md / CLAUDE2.md format**: Inserts directly below the date heading while preserving the existing file format. Does not conflict with manually written content.
