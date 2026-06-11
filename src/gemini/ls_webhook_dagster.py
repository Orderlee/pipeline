"""ls_webhook Dagster GraphQL 트리거."""

from __future__ import annotations

import requests

from gemini.ls_webhook_env import DAGSTER_GRAPHQL_URL


def _discover_repo_selector(graphql_url: str, job_name: str) -> dict | None:
    """repositoryLocations에서 job_name 보유 repo를 찾아 selector dict 반환."""
    query = """
    query { workspaceOrError { __typename ... on Workspace {
      locationEntries { locationOrLoadError { __typename
        ... on RepositoryLocation { name repositories { name pipelines { name } } }
      } }
    } } }
    """
    resp = requests.post(graphql_url, json={"query": query}, timeout=10)
    resp.raise_for_status()
    entries = resp.json().get("data", {}).get("workspaceOrError", {}).get("locationEntries", []) or []
    for entry in entries:
        loc = entry.get("locationOrLoadError") or {}
        if loc.get("__typename") != "RepositoryLocation":
            continue
        for repo in loc.get("repositories", []) or []:
            pipelines = [p.get("name") for p in repo.get("pipelines", []) or []]
            if job_name in pipelines:
                return {
                    "repositoryLocationName": loc.get("name"),
                    "repositoryName": repo.get("name"),
                    "jobName": job_name,
                }
    return None


def trigger_dagster_job(job_name: str, tags: dict[str, str]) -> tuple[bool, str]:
    """Dagster GraphQL launchPipelineExecution 트리거. (성공여부, 메시지) 반환."""
    try:
        selector = _discover_repo_selector(DAGSTER_GRAPHQL_URL, job_name)
    except Exception as exc:
        return False, f"repo discovery 실패: {exc}"
    if not selector:
        return False, f"job '{job_name}'을 포함한 repository를 찾지 못함"

    mutation = """
    mutation Launch($executionParams: ExecutionParams!) {
      launchPipelineExecution(executionParams: $executionParams) {
        __typename
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
        ... on PipelineNotFoundError { message }
        ... on RunConfigValidationInvalid { errors { message } }
        ... on InvalidSubsetError { message }
        ... on ConflictingExecutionParamsError { message }
      }
    }
    """
    variables = {
        "executionParams": {
            "selector": selector,
            "runConfigData": "{}",
            "mode": "default",
            "executionMetadata": {
                "tags": [{"key": k, "value": str(v)} for k, v in tags.items()],
            },
        }
    }
    try:
        resp = requests.post(
            DAGSTER_GRAPHQL_URL,
            json={"query": mutation, "variables": variables},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        return False, f"GraphQL 요청 실패: {exc}"

    result = (payload.get("data") or {}).get("launchPipelineExecution") or {}
    typename = result.get("__typename", "")
    if typename == "LaunchRunSuccess":
        run_id = (result.get("run") or {}).get("runId", "")
        return True, run_id
    return False, f"{typename}: {result.get('message', '') or result.get('errors', '')}"
