"""Shared Airflow callbacks for task failure notification + structured logging."""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("monogram_etl.airflow")


def on_failure(context: dict[str, Any]) -> None:
    """Fail-loud callback: logs context, optionally sends Slack webhook.

    Slack webhook is opt-in via the SLACK_WEBHOOK_URL env var. When unset, the
    callback still logs the failure to the Airflow task log so on-call has
    something to grep on.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception")
    try_number = task_instance.try_number if task_instance else 0

    logger.error(
        "Airflow task failed: dag=%s task=%s exec_date=%s try=%s exception=%s",
        dag_id, task_id, execution_date, try_number, exception,
    )

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    import json
    import urllib.request

    payload = {
        "text": (
            f":rotating_light: *Monogram ETL failure*\n"
            f"DAG: `{dag_id}`\nTask: `{task_id}`\n"
            f"Run: `{execution_date}` (try {try_number})\n"
            f"Error: ```{exception}```"
        )
    }
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=5)


def on_success(context: dict[str, Any]) -> None:
    """Brief success log; used on the final task of each DAG for SLA tracking."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    duration = task_instance.duration if task_instance else None
    logger.info("Airflow task succeeded: dag=%s task=%s duration=%ss", dag_id, task_id, duration)
