# Staging Empty Dispatch Request Waiting Handling

## Summary
Requests arriving at the `dispatch-agent` polling ingress where `labeling_method`, `outputs`, `run_mode`, `categories`, and `classes` are all empty are **not rejected** — they are held in a **waiting** state. While waiting, the incoming folder is left untouched and no dispatch run, manifest, or DB tracking row is created.

## Implementation Changes
- Add a **waiting determination function** to `production_agent_dispatch_sensor`.
  - Input: agent request payload
  - Condition: `labeling_method`, `outputs`, `run_mode`, `categories`, and `classes` are all empty
  - Result: waiting processing
- Waiting behavior is fixed as follows:
  - Do not call `prepare_dispatch_request()`
  - Do not call `write_dispatch_manifest()`
  - Do not call `build_dispatch_request_record()` / `build_dispatch_pipeline_rows()`
  - Do not create a `dispatch_stage_job` `RunRequest`
  - Do not create rows in `staging_dispatch_requests` or `staging_pipeline_runs`
  - Leave the incoming folder unchanged
  - Log `waiting_for_dispatch_params` in the sensor log
- The agent ack uses the existing endpoint as-is:
  - `status = "accepted"`
  - `message = "waiting_for_dispatch_params"`
- Rules for re-sending the same `request_id` are fixed as follows:
  - In waiting state there is no DB row, so no duplicate processing is performed
  - If a valid payload arrives again with the same `request_id`, proceed as a normal dispatch
  - After a normal dispatch has already been created, the existing duplicate/no-op rules apply
- Genuine failures remain unchanged:
  - Missing `source_unit_name` → `rejected`
  - Missing incoming folder → `rejected`
  - Payload shape error → `rejected`

## Test Plan
- Empty request waiting
  - When only `source_unit_name` is present and all other execution metadata is empty
  - `accepted + waiting_for_dispatch_params`
  - No manifest created
  - No RunRequest created
  - No rows in `staging_dispatch_requests` / `staging_pipeline_runs`
- Re-sending the same `request_id`
  - First request is waiting
  - On the second request, if `labeling_method` or `outputs/run_mode` and the required metadata are populated, a normal dispatch is created
- Failure cases remain unchanged
  - Missing `source_unit_name` → `rejected`
  - Missing incoming folder → `rejected`
- Regression check
  - Valid staging API payloads dispatch immediately as before
  - No impact on the production dispatch sensor

## Assumptions
- Scope is `staging only`.
- Waiting determination is fixed to the case where all metadata needed for an execution decision is empty.
- The waiting state is expressed solely as **keeping the incoming folder + an ack message** — not in the DB.
- A separate waiting-state table for UI/DB queries is not in scope for this change.
