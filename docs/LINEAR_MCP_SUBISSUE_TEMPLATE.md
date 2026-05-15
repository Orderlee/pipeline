# Linear MCP Sub-issue Creation Template

A template for automatically creating Linear sub-issues based on task descriptions.

## 1) Pre-flight Check

- Verify Linear MCP connection status (`linear` server is Running)
- Confirm workspace Team key (e.g. `ENG`, `DATA`)
- Have the parent issue identifier ready (`ABC-123` or issue UUID)

---

## 2) Preview Template Before Creation (Recommended)

Copy the block below as-is and fill in only the placeholders.

```text
You are a Linear issue designer.
Analyze the following task description and propose sub-issue drafts to place under the parent issue first.
Do not actually create anything yet.

[Input]
- Team: <TEAM_KEY>
- Parent issue: <PARENT_ISSUE_ID_OR_KEY>
- Task description:
<WORK_DESCRIPTION>
- Target completion date (optional): <YYYY-MM-DD or none>
- Default labels (optional): <label1,label2,...>
- Desired number of sub-issues: <N>

[Request]
1. Fetch the parent issue and incorporate its context.
2. Propose between 4 and 8 sub-issues.
3. Each sub-issue must include:
   - title
   - why (background/purpose)
   - scope (included)
   - out_of_scope (excluded)
   - acceptance_criteria (checklist of 2–5 items)
   - dependency (predecessor issue key or none)
   - recommended_priority (urgent/high/medium/low)
4. Present the results in a table sorted by execution order.
5. At the end, ask "Proceed with creation?" 
```

---

## 3) Immediate Creation Template

The version below requests actual creation after approval.

```text
You are a Linear issue operator.
Based on the following task description, create sub-issues under the parent issue.

[Input]
- Team: <TEAM_KEY>
- Parent issue: <PARENT_ISSUE_ID_OR_KEY>
- Task description:
<WORK_DESCRIPTION>
- Target completion date (optional): <YYYY-MM-DD or none>
- Default labels (optional): <label1,label2,...>
- Desired number of sub-issues: <N>

[Execution rules]
1. Fetch the parent issue and summarize its context.
2. Decompose the work into independent, non-overlapping sub-issues.
3. Each sub-issue description must include the following sections:
   - Context
   - Scope
   - Out of scope
   - Acceptance Criteria
   - Dependency
4. Actually create the sub-issues and link the parent-child relationship.
5. If there are dependencies, reflect the ordering between issues.
6. Return the results in a table with:
   - issue key
   - title
   - url
   - dependency
   - suggested order

[Important]
- If anything is ambiguous, ask a clarifying question only once before creating.
- Do not create duplicate issues.
- Each issue must include verifiable acceptance criteria.
```

---

## 4) Quick Example

```text
Team: DATA
Parent issue: DATA-421
Task description: Structure the failure logs from the staging dispatch sensor and wire them through to retry/alerting/dashboard
Default labels: staging,dispatch,observability
Desired number of sub-issues: 6
```
