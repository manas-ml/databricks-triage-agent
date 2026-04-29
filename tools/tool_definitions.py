ALL_TOOLS = [
    # -------------------------------------------------------------------------
    # Diagnostic tools (read-only)
    # -------------------------------------------------------------------------
    {
        "name": "get_job_run_details",
        "description": (
            "Retrieve details for a specific Databricks job run including state, "
            "error message, cluster ID, start/end times, and duration in minutes. "
            "Use this as the first diagnostic step when given a run_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "integer",
                    "description": "The Databricks job run ID to inspect.",
                }
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_job_run_logs",
        "description": (
            "Fetch stdout, stderr, or driver logs for a Databricks job run. "
            "Returns the last 8000 characters of the requested log type. "
            "Always check stderr/driver logs when diagnosing failures."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "integer",
                    "description": "The Databricks job run ID.",
                },
                "log_type": {
                    "type": "string",
                    "enum": ["stdout", "stderr", "driver"],
                    "description": "Type of log to retrieve. Default: stderr.",
                },
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_cluster_details",
        "description": (
            "Get configuration details for a Databricks cluster including node type, "
            "memory, number of workers, autoscaling config, Spark version, and "
            "Spark configuration. Use this to diagnose OOM and sizing issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The Databricks cluster ID.",
                }
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "list_recent_job_runs",
        "description": (
            "List recent runs for a Databricks job to identify failure patterns, "
            "failure rate, and recurring error messages. Useful for distinguishing "
            "transient failures from systematic issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "The Databricks job ID.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent runs to return (max 25). Default: 10.",
                },
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "get_job_definition",
        "description": (
            "Retrieve the full configuration for a Databricks job including tasks, "
            "cluster specifications, libraries, parameters, schedule, and retry settings. "
            "Use this to understand the job structure and verify applied fixes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "The Databricks job ID.",
                }
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "run_sql_query",
        "description": (
            "Execute a SQL query against a Databricks SQL warehouse for data quality "
            "diagnostics, table inspection, or validation. Returns up to 500 rows. "
            "Use for DESCRIBE DETAIL, data profiling, NULL checks, and constraint validation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL statement to execute.",
                },
                "warehouse_id": {
                    "type": "string",
                    "description": (
                        "The SQL warehouse ID to use. If omitted, uses the default "
                        "warehouse from DATABRICKS_SQL_WAREHOUSE_ID env var."
                    ),
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_delta_table_details",
        "description": (
            "Get comprehensive details for a Delta Lake table including schema, "
            "DESCRIBE DETAIL output (size, num files, location), and the last 10 "
            "DESCRIBE HISTORY entries. Use for diagnosing schema evolution issues, "
            "concurrent write conflicts, and data corruption."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": (
                        "Fully qualified table name, e.g. 'catalog.schema.table' "
                        "or 'schema.table'."
                    ),
                }
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "get_notebook_source",
        "description": (
            "Export and read the source code of a Databricks notebook or script "
            "from the workspace. Use to inspect code for bugs, anti-patterns, "
            "or logic errors that may be causing pipeline failures."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": (
                        "Absolute Databricks workspace path, "
                        "e.g. '/Users/user@example.com/etl_pipeline'."
                    ),
                }
            },
            "required": ["path"],
        },
    },
    # -------------------------------------------------------------------------
    # Fix tools
    # -------------------------------------------------------------------------
    {
        "name": "update_job_cluster_config",
        "description": (
            "Update the cluster configuration for a Databricks job to fix sizing, "
            "memory, or autoscaling issues. Merges the provided cluster_config into "
            "the existing configuration without overwriting unspecified fields. "
            "Returns before/after diff. Use for OOM fixes and performance improvements."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "The Databricks job ID to update.",
                },
                "cluster_config": {
                    "type": "object",
                    "description": (
                        "Cluster configuration fields to update, e.g. "
                        "{'num_workers': 8, 'node_type_id': 'i3.2xlarge', "
                        "'autoscale': {'min_workers': 2, 'max_workers': 10}}."
                    ),
                },
                "task_key": {
                    "type": "string",
                    "description": (
                        "If the job has multiple tasks with different clusters, "
                        "specify the task key to target. Omit to update the "
                        "job-level cluster."
                    ),
                },
            },
            "required": ["job_id", "cluster_config"],
        },
    },
    {
        "name": "update_job_settings",
        "description": (
            "Update operational settings for a Databricks job such as retry policy, "
            "timeout, and concurrency. Use to fix timeout failures and add retry logic. "
            "Returns before/after diff of changed fields."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "The Databricks job ID to update.",
                },
                "settings": {
                    "type": "object",
                    "description": (
                        "Settings to update. Supported keys: "
                        "max_retries (int), timeout_seconds (int), "
                        "retry_on_timeout (bool), max_concurrent_runs (int)."
                    ),
                },
            },
            "required": ["job_id", "settings"],
        },
    },
    {
        "name": "set_spark_config",
        "description": (
            "Add or update Spark configuration parameters for a Databricks job. "
            "Merges new key-value pairs into the existing spark_conf without removing "
            "existing settings. Use for adaptive query execution, shuffle tuning, "
            "and Delta-specific configs. Returns merged config."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "The Databricks job ID to update.",
                },
                "spark_conf": {
                    "type": "object",
                    "description": (
                        "Spark configuration key-value pairs to add or update, e.g. "
                        "{'spark.sql.adaptive.enabled': 'true', "
                        "'spark.sql.shuffle.partitions': '400'}."
                    ),
                },
                "task_key": {
                    "type": "string",
                    "description": (
                        "If the job has multiple tasks, specify the task key to target. "
                        "Omit to update the job-level cluster spark_conf."
                    ),
                },
            },
            "required": ["job_id", "spark_conf"],
        },
    },
    {
        "name": "update_table_properties",
        "description": (
            "Set Delta Lake table properties using ALTER TABLE SET TBLPROPERTIES. "
            "Use to enable schema evolution, change isolation level, enable CDC, "
            "or configure other Delta table behaviours. Returns the SQL executed and status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name, e.g. 'catalog.schema.table'.",
                },
                "properties": {
                    "type": "object",
                    "description": (
                        "Table properties to set as key-value pairs, e.g. "
                        "{'delta.enableChangeDataFeed': 'true', "
                        "'delta.isolationLevel': 'Serializable', "
                        "'delta.columnMapping.mode': 'name'}."
                    ),
                },
                "warehouse_id": {
                    "type": "string",
                    "description": "SQL warehouse ID to execute the ALTER TABLE statement.",
                },
            },
            "required": ["table_name", "properties"],
        },
    },
]
