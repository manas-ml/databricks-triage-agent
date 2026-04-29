"""
Databricks tool implementations using the Databricks Python SDK (WorkspaceClient).
Each method maps to one tool schema in tool_definitions.py.
All methods return JSON-serialisable dicts; the execute() dispatcher JSON-encodes them.
"""
import base64
import json
import time
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, Task, ClusterSpec
from databricks.sdk.service.sql import StatementState


class DatabricksToolExecutor:
    def __init__(
        self,
        host: str,
        token: str,
        default_warehouse_id: str | None = None,
    ):
        self.client = WorkspaceClient(host=host, token=token)
        self.default_warehouse_id = default_warehouse_id

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def execute(self, tool_name: str, tool_input: dict) -> str:
        dispatch = {
            "get_job_run_details": self.get_job_run_details,
            "get_job_run_logs": self.get_job_run_logs,
            "get_cluster_details": self.get_cluster_details,
            "list_recent_job_runs": self.list_recent_job_runs,
            "get_job_definition": self.get_job_definition,
            "run_sql_query": self.run_sql_query,
            "get_delta_table_details": self.get_delta_table_details,
            "get_notebook_source": self.get_notebook_source,
            "update_job_cluster_config": self.update_job_cluster_config,
            "update_job_settings": self.update_job_settings,
            "set_spark_config": self.set_spark_config,
            "update_table_properties": self.update_table_properties,
        }
        fn = dispatch.get(tool_name)
        if fn is None:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        try:
            result = fn(**tool_input)
            return json.dumps(result, default=str)
        except Exception as exc:
            return json.dumps({"error": str(exc), "tool": tool_name})

    # ------------------------------------------------------------------
    # Diagnostic tools
    # ------------------------------------------------------------------

    def get_job_run_details(self, run_id: int) -> dict:
        run = self.client.jobs.get_run(run_id=run_id)
        state = run.state

        start_ms = run.start_time or 0
        end_ms = run.end_time or int(time.time() * 1000)
        duration_minutes = round((end_ms - start_ms) / 60000, 2) if start_ms else None

        cluster_id = None
        if run.cluster_instance:
            cluster_id = run.cluster_instance.cluster_id

        job_id = run.job_id
        run_page_url = run.run_page_url

        tasks_summary = []
        if run.tasks:
            for task in run.tasks:
                t_state = task.state
                tasks_summary.append(
                    {
                        "task_key": task.task_key,
                        "state": t_state.result_state.value if t_state and t_state.result_state else None,
                        "error": t_state.state_message if t_state else None,
                        "cluster_id": (
                            task.cluster_instance.cluster_id
                            if task.cluster_instance
                            else None
                        ),
                    }
                )

        return {
            "run_id": run_id,
            "job_id": job_id,
            "run_page_url": run_page_url,
            "life_cycle_state": state.life_cycle_state.value if state and state.life_cycle_state else None,
            "result_state": state.result_state.value if state and state.result_state else None,
            "error_message": state.state_message if state else None,
            "cluster_id": cluster_id,
            "start_time": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat() if start_ms else None,
            "end_time": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat() if run.end_time else None,
            "duration_minutes": duration_minutes,
            "tasks": tasks_summary,
        }

    def get_job_run_logs(self, run_id: int, log_type: str = "stderr") -> dict:
        output = self.client.jobs.get_run_output(run_id=run_id)
        content = ""

        if log_type == "stdout":
            content = output.logs or ""
        elif log_type == "stderr":
            parts = []
            if output.error:
                parts.append(f"ERROR: {output.error}")
            if output.error_trace:
                parts.append(output.error_trace)
            content = "\n".join(parts)
        elif log_type == "driver":
            # Driver logs: prefer error_trace, fall back to logs
            content = output.error_trace or output.logs or ""

        truncated = len(content) > 8000
        return {
            "run_id": run_id,
            "log_type": log_type,
            "content": content[-8000:] if truncated else content,
            "truncated": truncated,
            "total_length": len(content),
        }

    def get_cluster_details(self, cluster_id: str) -> dict:
        cluster = self.client.clusters.get(cluster_id=cluster_id)

        autoscale = None
        if cluster.autoscale:
            autoscale = {
                "min_workers": cluster.autoscale.min_workers,
                "max_workers": cluster.autoscale.max_workers,
            }

        init_scripts = []
        if cluster.init_scripts:
            for script in cluster.init_scripts:
                if script.dbfs:
                    init_scripts.append({"type": "dbfs", "path": script.dbfs.destination})
                elif script.workspace:
                    init_scripts.append({"type": "workspace", "path": script.workspace.destination})
                elif script.volumes:
                    init_scripts.append({"type": "volumes", "path": script.volumes.destination})

        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster.cluster_name,
            "state": cluster.state.value if cluster.state else None,
            "spark_version": cluster.spark_version,
            "node_type_id": cluster.node_type_id,
            "driver_node_type_id": cluster.driver_node_type_id,
            "num_workers": cluster.num_workers,
            "autoscale": autoscale,
            "spark_conf": dict(cluster.spark_conf) if cluster.spark_conf else {},
            "init_scripts": init_scripts,
            "runtime_engine": cluster.runtime_engine.value if cluster.runtime_engine else None,
            "data_security_mode": cluster.data_security_mode.value if cluster.data_security_mode else None,
        }

    def list_recent_job_runs(self, job_id: int, limit: int = 10) -> dict:
        limit = min(limit, 25)
        runs_iter = self.client.jobs.list_runs(job_id=job_id, limit=limit)
        runs = list(runs_iter)

        run_summaries = []
        failed = 0
        for run in runs:
            state = run.state
            result_state = state.result_state.value if state and state.result_state else None
            if result_state == "FAILED":
                failed += 1
            start_ms = run.start_time or 0
            end_ms = run.end_time or 0
            duration_seconds = round((end_ms - start_ms) / 1000, 1) if start_ms and end_ms else None
            run_summaries.append(
                {
                    "run_id": run.run_id,
                    "start_time": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat() if start_ms else None,
                    "duration_seconds": duration_seconds,
                    "result_state": result_state,
                    "error_summary": (state.state_message[:200] if state and state.state_message else None),
                }
            )

        failure_rate = f"{round(failed / len(runs) * 100)}%" if runs else "N/A"
        return {
            "job_id": job_id,
            "runs": run_summaries,
            "total_returned": len(runs),
            "failure_count": failed,
            "failure_rate": failure_rate,
        }

    def get_job_definition(self, job_id: int) -> dict:
        job = self.client.jobs.get(job_id=job_id)
        settings = job.settings

        tasks = []
        if settings and settings.tasks:
            for task in settings.tasks:
                task_dict: dict[str, Any] = {"task_key": task.task_key}
                if task.new_cluster:
                    task_dict["cluster"] = {
                        "node_type_id": task.new_cluster.node_type_id,
                        "num_workers": task.new_cluster.num_workers,
                        "spark_conf": dict(task.new_cluster.spark_conf) if task.new_cluster.spark_conf else {},
                    }
                if task.existing_cluster_id:
                    task_dict["existing_cluster_id"] = task.existing_cluster_id
                if task.notebook_task:
                    task_dict["notebook_path"] = task.notebook_task.notebook_path
                if task.python_wheel_task:
                    task_dict["python_wheel_task"] = {
                        "package_name": task.python_wheel_task.package_name,
                        "entry_point": task.python_wheel_task.entry_point,
                    }
                if task.spark_python_task:
                    task_dict["python_file"] = task.spark_python_task.python_file
                tasks.append(task_dict)

        job_cluster = None
        if settings and settings.job_clusters:
            jc = settings.job_clusters[0]
            if jc.new_cluster:
                job_cluster = {
                    "job_cluster_key": jc.job_cluster_key,
                    "node_type_id": jc.new_cluster.node_type_id,
                    "num_workers": jc.new_cluster.num_workers,
                    "autoscale": (
                        {"min_workers": jc.new_cluster.autoscale.min_workers,
                         "max_workers": jc.new_cluster.autoscale.max_workers}
                        if jc.new_cluster.autoscale else None
                    ),
                    "spark_conf": dict(jc.new_cluster.spark_conf) if jc.new_cluster.spark_conf else {},
                }

        retry_policy = None
        if settings and settings.tasks:
            for task in settings.tasks:
                if task.max_retries is not None:
                    retry_policy = {
                        "max_retries": task.max_retries,
                        "retry_on_timeout": task.retry_on_timeout,
                        "timeout_seconds": task.timeout_seconds,
                    }
                    break

        return {
            "job_id": job_id,
            "name": settings.name if settings else None,
            "job_cluster": job_cluster,
            "tasks": tasks,
            "retry_policy": retry_policy,
            "max_concurrent_runs": settings.max_concurrent_runs if settings else None,
            "schedule": str(settings.schedule) if settings and settings.schedule else None,
        }

    def run_sql_query(self, sql: str, warehouse_id: str | None = None) -> dict:
        wid = warehouse_id or self.default_warehouse_id
        if not wid:
            return {"error": "No warehouse_id provided and DATABRICKS_SQL_WAREHOUSE_ID not set."}

        stmt = self.client.statement_execution.execute_statement(
            warehouse_id=wid,
            statement=sql,
            wait_timeout="30s",
        )

        # Poll until done (max 60s extra beyond the 30s wait_timeout)
        deadline = time.time() + 60
        while stmt.status and stmt.status.state not in (
            StatementState.SUCCEEDED,
            StatementState.FAILED,
            StatementState.CANCELED,
            StatementState.CLOSED,
        ):
            if time.time() > deadline:
                return {"error": "SQL query timed out after 90 seconds."}
            time.sleep(2)
            stmt = self.client.statement_execution.get_statement(stmt.statement_id)

        if not stmt.status or stmt.status.state != StatementState.SUCCEEDED:
            error_msg = str(stmt.status.error) if stmt.status and stmt.status.error else "Unknown error"
            return {"error": f"SQL query failed: {error_msg}", "sql": sql}

        columns = []
        rows = []
        if stmt.manifest and stmt.manifest.schema and stmt.manifest.schema.columns:
            columns = [col.name for col in stmt.manifest.schema.columns]
        if stmt.result and stmt.result.data_array:
            rows = stmt.result.data_array[:500]

        return {
            "sql": sql,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": len(stmt.result.data_array) > 500 if stmt.result and stmt.result.data_array else False,
        }

    def get_delta_table_details(self, table_name: str) -> dict:
        detail = self.run_sql_query(f"DESCRIBE DETAIL {table_name}")
        history = self.run_sql_query(f"DESCRIBE HISTORY {table_name} LIMIT 10")
        schema = self.run_sql_query(f"DESCRIBE TABLE {table_name}")
        return {
            "table_name": table_name,
            "detail": detail,
            "history": history,
            "schema": schema,
        }

    def get_notebook_source(self, path: str) -> dict:
        from databricks.sdk.service.workspace import ExportFormat

        exported = self.client.workspace.export(path=path, format=ExportFormat.SOURCE)
        raw = exported.content or ""
        source = base64.b64decode(raw).decode("utf-8") if raw else ""
        lines = source.splitlines()
        return {
            "path": path,
            "source": source,
            "line_count": len(lines),
        }

    # ------------------------------------------------------------------
    # Fix tools
    # ------------------------------------------------------------------

    def update_job_cluster_config(
        self,
        job_id: int,
        cluster_config: dict,
        task_key: str | None = None,
    ) -> dict:
        job = self.client.jobs.get(job_id=job_id)
        settings = job.settings
        if not settings:
            return {"error": "Job has no settings."}

        before: dict = {}
        after: dict = {}

        if task_key and settings.tasks:
            # Update a specific task's new_cluster
            updated_tasks = []
            for task in settings.tasks:
                if task.task_key == task_key and task.new_cluster:
                    before = _cluster_to_dict(task.new_cluster)
                    merged = {**before, **cluster_config}
                    task = Task.from_dict({**task.as_dict(), "new_cluster": merged})
                    after = merged
                updated_tasks.append(task)
            self.client.jobs.update(
                job_id=job_id,
                new_settings=JobSettings(tasks=updated_tasks),
            )
        elif settings.job_clusters:
            # Update the first job-level cluster
            jc = settings.job_clusters[0]
            if jc.new_cluster:
                before = _cluster_to_dict(jc.new_cluster)
                merged = {**before, **cluster_config}
                after = merged
                updated_jc = jc.as_dict()
                updated_jc["new_cluster"] = merged
                self.client.jobs.update(
                    job_id=job_id,
                    new_settings=JobSettings.from_dict(
                        {**settings.as_dict(), "job_clusters": [updated_jc]}
                    ),
                )
        elif settings.tasks and settings.tasks[0].new_cluster:
            # Inline cluster on first task
            task = settings.tasks[0]
            before = _cluster_to_dict(task.new_cluster)
            merged = {**before, **cluster_config}
            after = merged
            updated_task = task.as_dict()
            updated_task["new_cluster"] = merged
            updated_tasks = [Task.from_dict(updated_task)] + list(settings.tasks[1:])
            self.client.jobs.update(
                job_id=job_id,
                new_settings=JobSettings(tasks=updated_tasks),
            )
        else:
            return {
                "error": "Could not locate a new_cluster to update. Job may use an existing cluster.",
                "job_id": job_id,
            }

        return {"status": "success", "job_id": job_id, "before": before, "after": after}

    def update_job_settings(self, job_id: int, settings: dict) -> dict:
        job = self.client.jobs.get(job_id=job_id)
        job_settings = job.settings
        if not job_settings or not job_settings.tasks:
            return {"error": "Job has no tasks to update settings on."}

        supported = {"max_retries", "timeout_seconds", "retry_on_timeout", "max_concurrent_runs"}
        unknown = set(settings.keys()) - supported
        if unknown:
            return {"error": f"Unsupported settings keys: {unknown}. Supported: {supported}"}

        before: dict = {}
        updated_tasks = []
        for task in job_settings.tasks:
            before = {
                "max_retries": task.max_retries,
                "timeout_seconds": task.timeout_seconds,
                "retry_on_timeout": task.retry_on_timeout,
            }
            task_dict = task.as_dict()
            for k, v in settings.items():
                if k in ("max_retries", "timeout_seconds", "retry_on_timeout"):
                    task_dict[k] = v
            updated_tasks.append(Task.from_dict(task_dict))

        new_settings_dict = job_settings.as_dict()
        new_settings_dict["tasks"] = [t.as_dict() for t in updated_tasks]
        if "max_concurrent_runs" in settings:
            new_settings_dict["max_concurrent_runs"] = settings["max_concurrent_runs"]

        self.client.jobs.update(
            job_id=job_id,
            new_settings=JobSettings.from_dict(new_settings_dict),
        )

        after = {k: settings.get(k, before.get(k)) for k in before}
        if "max_concurrent_runs" in settings:
            after["max_concurrent_runs"] = settings["max_concurrent_runs"]

        return {
            "status": "success",
            "job_id": job_id,
            "before": before,
            "after": after,
        }

    def set_spark_config(
        self,
        job_id: int,
        spark_conf: dict,
        task_key: str | None = None,
    ) -> dict:
        job = self.client.jobs.get(job_id=job_id)
        settings = job.settings
        if not settings:
            return {"error": "Job has no settings."}

        before: dict = {}
        after: dict = {}

        def _merge(existing: dict | None) -> dict:
            return {**(dict(existing) if existing else {}), **spark_conf}

        if task_key and settings.tasks:
            updated_tasks = []
            for task in settings.tasks:
                if task.task_key == task_key and task.new_cluster:
                    before = dict(task.new_cluster.spark_conf or {})
                    after = _merge(before)
                    task_dict = task.as_dict()
                    task_dict["new_cluster"]["spark_conf"] = after
                    task = Task.from_dict(task_dict)
                updated_tasks.append(task)
            self.client.jobs.update(
                job_id=job_id,
                new_settings=JobSettings(tasks=updated_tasks),
            )
        elif settings.job_clusters:
            jc = settings.job_clusters[0]
            before = dict(jc.new_cluster.spark_conf or {}) if jc.new_cluster else {}
            after = _merge(before)
            updated_jc = jc.as_dict()
            if "new_cluster" not in updated_jc:
                updated_jc["new_cluster"] = {}
            updated_jc["new_cluster"]["spark_conf"] = after
            new_settings_dict = settings.as_dict()
            new_settings_dict["job_clusters"] = [updated_jc]
            self.client.jobs.update(
                job_id=job_id,
                new_settings=JobSettings.from_dict(new_settings_dict),
            )
        elif settings.tasks and settings.tasks[0].new_cluster:
            task = settings.tasks[0]
            before = dict(task.new_cluster.spark_conf or {})
            after = _merge(before)
            task_dict = task.as_dict()
            task_dict["new_cluster"]["spark_conf"] = after
            updated_tasks = [Task.from_dict(task_dict)] + list(settings.tasks[1:])
            self.client.jobs.update(
                job_id=job_id,
                new_settings=JobSettings(tasks=updated_tasks),
            )
        else:
            return {"error": "Could not locate a cluster with spark_conf to update."}

        added = {k: v for k, v in spark_conf.items() if k not in before}
        updated_keys = {k: v for k, v in spark_conf.items() if k in before and before[k] != v}

        return {
            "status": "success",
            "job_id": job_id,
            "added_keys": added,
            "updated_keys": updated_keys,
            "full_spark_conf": after,
        }

    def update_table_properties(
        self,
        table_name: str,
        properties: dict,
        warehouse_id: str | None = None,
    ) -> dict:
        props_clause = ", ".join(
            f"'{k}' = '{v}'" for k, v in properties.items()
        )
        sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props_clause})"
        result = self.run_sql_query(sql, warehouse_id=warehouse_id)
        if "error" in result:
            return {"status": "error", "error": result["error"], "sql": sql}
        return {
            "status": "success",
            "table_name": table_name,
            "properties_set": properties,
            "sql_executed": sql,
        }


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _cluster_to_dict(cluster) -> dict:
    """Safely convert a ClusterSpec or similar object to a plain dict."""
    if hasattr(cluster, "as_dict"):
        return cluster.as_dict()
    return dict(cluster) if cluster else {}
