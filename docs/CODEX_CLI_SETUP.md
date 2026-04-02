# Codex CLI 설정 방법

OpenAI Codex CLI 설치·인증·기본 사용법 정리.  
공식 문서: [Codex CLI](https://developers.openai.com/codex/cli), [Quickstart](https://developers.openai.com/codex/quickstart)

---

## 1. 사전 요구사항

- **Node.js** 18 이상 (권장 20)
- **npm** 8 이상
- **OpenAI 계정**  
  - [platform.openai.com](https://platform.openai.com) API 크레딧, 또는  
  - ChatGPT Plus/Pro/Enterprise 구독 (Codex 포함)

---

## 2. 설치

### 전역 설치 (권장)

```bash
npm install -g @openai/codex
codex --version
```

### npx로 실행만 (설치 없이)

```bash
npx @openai/codex --version
```

---

## 3. 인증

### 방법 A: 대화형 로그인 (권장)

```bash
codex auth
```

시스템 키체인에 자격 증명이 저장됩니다.

### 방법 B: 환경 변수

```bash
export OPENAI_API_KEY="sk-..."
# 영구 적용 (bash)
echo 'export OPENAI_API_KEY="sk-..."' >> ~/.bashrc
source ~/.bashrc
```

### 확인

```bash
codex auth status
```

---

## 4. 설정 파일 (선택)

- **전역:** `~/.codex/config.toml`
- **프로젝트:** `.codex/config.toml` (해당 repo에서만 적용, 우선)

MCP 서버, 모델 기본값, feature flag 등 설정.  
예: [Config Reference](https://developers.openai.com/codex/config-reference)

---

## 5. 서브에이전트 (선택)

- **전역:** `~/.codex/agents/` — 모든 프로젝트
- **프로젝트:** `.codex/agents/` — 현재 repo만, **우선**

이 프로젝트에서는 다음이 적용되어 있습니다.

- **`.codex/config.toml`**  
  - `[features] multi_agent = true` 로 서브에이전트 사용 가능  
  - `[agents.이름]` 으로 `.codex/agents/` 및 `categories/` 하위 모든 `.toml` 등록 (`config_file` 경로)
- **에이전트 소스:** `.codex/agents/*.toml` + `.codex/agents/categories/**/*.toml` (awesome-codex-subagents)

Codex(CLI 또는 MCP)를 **이 repo 루트를 작업 디렉터리(cwd)** 로 실행해야 `.codex/config.toml`과 `agents/` 경로가 올바르게 잡힙니다.

**MCP(codex-cli-mcp)에서 cwd 동작 (확인 결과)**  
`@nayagamez/codex-cli-mcp`는 Codex를 호출할 때 **`cwd` 인자를 넘기면** Codex CLI에 `-C <cwd>`만 전달합니다. `spawn` 쪽에는 `cwd`를 주지 않아서, **`cwd`를 넘기지 않으면** Codex는 **MCP 서버 프로세스의 현재 작업 디렉터리**에서 실행됩니다.  
→ Cursor가 MCP 서버를 **워크스페이스 루트를 cwd로** 띄우면 프로젝트 에이전트가 로드될 수 있고, 그렇지 않으면 로드되지 않을 수 있습니다.  
→ **권장:** Codex MCP 도구를 호출할 때 **`cwd`에 이 repo 루트 경로를 넣어서** 호출하면, 어떤 환경에서든 프로젝트 에이전트가 확실히 로드됩니다. (예: Cursor 에이전트가 `codex` 도구 호출 시 `cwd: "/home/pia/work_p/Datapipeline-Data-data_pipeline"` 또는 `workspaceRoot` 전달)

---

## 6. MCP (Codex 쪽 설정)

Codex **자체**에서 MCP 서버를 쓰려면:

- `~/.codex/config.toml` 또는 `.codex/config.toml` 에 `[mcp_servers.이름]` 추가
- 또는 CLI: `codex mcp add <이름> -- <stdio 명령>`

**Cursor에서 Codex를 MCP로 쓰는 경우**  
→ 이 repo의 `.cursor/mcp.json` / `.agent/mcp/mcp_config.json` 에 이미 `codex` (codex-cli-mcp) 가 등록되어 있음.  
Codex CLI가 PATH에 있고 `codex auth` 완료되어 있으면 Cursor가 Codex MCP로 호출 가능.

---

## 7. 기본 사용

| 용도 | 명령 |
|------|------|
| 대화형 세션 | `codex` |
| 한 번만 질문 후 종료 | `codex "이 코드베이스 설명해줘"` |
| 비대화형 실행 (스크립트용) | `codex exec "CI 실패 수정해줘"` |
| 특정 디렉터리 기준 | `codex --cd /path/to/repo "..."` |
| 모델 지정 | `codex --model gpt-5.4 "..."` |
| 세션 이어하기 | `codex resume` / `codex resume --last` |

---

## 8. 이 프로젝트에서

- **Cursor MCP:** `codex` 서버 = `@nayagamez/codex-cli-mcp` (Codex CLI 래퍼)
- **서브에이전트:** `.codex/agents/` 가 로드되려면 Codex가 **이 repo 루트를 cwd**로 실행되어야 함. MCP 호출 시 `cwd` 인자로 repo 루트를 넘기면 확실함 (위 "MCP에서 cwd 동작" 참고).
- **동기화:** MCP 설정은 `.agent/mcp/mcp_config.json` 이 마스터 → `sync_mcp.sh` 로 `.cursor/mcp.json` 등 반영

```bash
# 설치·인증 확인
codex --version
codex auth status
```
