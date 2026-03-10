"""Helpers for emitting Databricks job task values."""

from __future__ import annotations


def set_task_value(spark, key: str, value: object) -> None:
    """Set a Databricks job task value. No-op outside a job context."""
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass
