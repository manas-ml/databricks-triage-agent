# Databricks ETL Pipeline Triage Agent

An AI-powered triage agent built with the [Anthropic Claude Agent SDK](https://docs.anthropic.com/en/docs/agents) that automatically diagnoses Databricks ETL pipeline failures, identifies root causes, applies fixes, and re-triggers the job — all in a single run.

## How It Works

The agent runs a tool-use loop powered by `claude-sonnet-4-6`. In each iteration, Claude can call multiple tools simultaneously (parallel tool use), enabling it to apply several fixes in one pass before producing a final report.

```
Diagnose → Classify Root Cause → Investigate → Fix (parallel) → Verify → Re-run → Report
```

## Features

- **Root cause classification** across 9 failure categories (OOM, schema mismatch, Delta conflicts, timeouts, permissions, library errors, data quality, write failures, storage errors)
- **Automated fixes** applied directly to the Databricks workspace via the SDK — no manual intervention needed
- **Multiple fixes per loop iteration** — all fix tool calls in a single Claude response are batched and executed together
- **Job re-trigger** after successful fixes using `jobs.run_now()`
- **Structured triage report** with root cause analysis, before/after diffs, re-run status, and severity assessment
- **CLI interface** with verbose mode and JSON output for downstream integration

## Tools (13 total)

### Diagnostic (read-only)

| Tool | Purpose |
|---|---|
| `get_job_run_details` | Run state, error message, cluster ID, duration |
| `get_job_run_logs` | stdout / stderr / driver logs (last 8 000 chars) |
| `get_cluster_details` | Node type, memory, worker count, Spark config |
| `list_recent_job_runs` | Run history and failure rate for a job |
| `get_job_definition` | Full job config: tasks, cluster spec, libraries, retry policy |
| `run_sql_query` | Diagnostic SQL against a SQL warehouse |
| `get_delta_table_details` | DESCRIBE DETAIL + DESCRIBE HISTORY + schema |
| `get_notebook_source` | Notebook / script source code for code-level analysis |

### Fix

| Tool | What It Changes |
|---|---|
| `update_job_cluster_config` | Cluster sizing, node type, autoscaling |
| `update_job_settings` | Retries, timeout, max concurrent runs |
| `set_spark_config` | Spark configuration parameters (merged, non-destructive) |
| `update_table_properties` | Delta Lake table properties via `ALTER TABLE SET TBLPROPERTIES` |

### Action

| Tool | Purpose |
|---|---|
| `trigger_job_run` | Re-trigger the job with `jobs.run_now()` after fixes are verified |

## Root Cause Taxonomy

| Failure Pattern | Root Cause | Auto-Fixed |
|---|---|---|
| `OutOfMemoryError`, `Container killed by YARN`, `GC overhead` | OOM — executor or driver | ✅ |
| `ConcurrentAppendException`, `ConcurrentDeleteReadException` | Delta concurrent write conflict | ✅ |
| `TIMEOUT`, `socket timeout`, `Job aborted` | Network / timeout / transient failure | ✅ |
| `AnalysisException: cannot resolve column`, schema mismatch | Schema evolution / mismatch | ✅ (partial) |
| `NullPointerException` on rows, `MALFORMED`, `Bad record` | Data quality / type mismatch | ✅ (partial) |
| `PermissionDenied`, `403 Forbidden` | Permission error | ❌ — recommendation only |
| `ImportError`, `ClassNotFoundException` | Library / dependency conflict | ❌ — recommendation only |
| `FileNotFoundException`, `Path does not exist` | Missing file / storage path | ❌ — recommendation only |

## Installation

```bash
git clone <repo-url>
cd databricks-triage-agent
pip install -r requirements.txt
cp .env.example .env   # fill in your credentials
```

## Configuration

Edit `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
DATABRICKS_SQL_WAREHOUSE_ID=abc123def456   # optional — needed for SQL-based tools
```

## Usage

```bash
# Triage a specific failed run
python main.py --run-id 12345

# Triage by job ID (agent finds the most recent failed run)
python main.py --job-id 99

# Provide both for richer context
python main.py --run-id 12345 --job-id 99

# Verbose mode — prints each tool call and result as the agent runs
python main.py --run-id 12345 --verbose

# JSON output for downstream processing
python main.py --run-id 12345 --output json > report.json
```

## Example Report

```
## TRIAGE REPORT

### Issue Summary
Job 99 run 12345 failed after 4.2 minutes in the `transform_orders` task
with an executor OOM error on a 4-node i3.xlarge cluster.

### Root Cause Analysis
- **Root Cause 1**: OOM — executor out of memory
  - Evidence: `java.lang.OutOfMemoryError: Java heap space` in executor logs
  - Impact: Spark task retry storm leading to full job abort

### Applied Fixes
- **Fix 1**: update_job_cluster_config — increased num_workers 4 → 8
  - Before: `{num_workers: 4, node_type_id: "i3.xlarge"}`
  - After:  `{num_workers: 8, node_type_id: "i3.xlarge"}`
  - Status: ✓ Verified
- **Fix 2**: set_spark_config — enabled adaptive query execution
  - Before: `{}`
  - After:  `{"spark.sql.adaptive.enabled": "true"}`
  - Status: ✓ Verified

### Re-run Status
- **Triggered**: New run ID 12346 — https://workspace.azuredatabricks.net/#job/99/run/12346

### Remaining Recommendations
None.

### Severity Assessment
**HIGH** — Pipeline was consistently failing due to undersized cluster; fix applied and job re-triggered.
```

## Project Structure

```
databricks-triage-agent/
├── agent.py                    # TriageAgent — core Claude tool-use loop
├── main.py                     # CLI entry point (click + rich)
├── prompts.py                  # System prompt and root cause taxonomy
├── requirements.txt
├── .env.example
└── tools/
    ├── __init__.py
    ├── tool_definitions.py     # Claude API tool schemas (ALL_TOOLS)
    └── databricks_tools.py     # DatabricksToolExecutor — Databricks SDK calls
```

## Architecture Notes

**Multi-fix per loop iteration**: When Claude returns multiple `tool_use` blocks in a single response, `_execute_tool_batch()` in `agent.py` runs all of them and returns their results as one batched user message. This lets the agent apply `fix1 + fix2 + fix3` simultaneously rather than one per round-trip.

**Prompt caching**: The system prompt is passed with `cache_control: {"type": "ephemeral"}`, reducing latency and token cost on iterations 2+ of the same agent run.

**Non-destructive updates**: Fix tools use `jobs.update()` (partial update) rather than `jobs.reset()`, so only the specified fields change — the rest of your job config is untouched.
