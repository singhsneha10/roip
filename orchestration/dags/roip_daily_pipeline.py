"""
roip_daily_pipeline.py
───────────────────────
Daily batch pipeline DAG.

Schedule: runs at 02:00 UTC daily, processing the previous day's data.

Task flow:
  bronze_to_silver → silver_to_gold → reconciliation → dq_gate
                  ↘ customer_dim_scd2               ↗

Airflow concepts demonstrated:
  - task dependencies with >> and 
  - BashOperator for Spark job submission
  - conditional branching on DQ results
  - SLAs and alerting hooks
  - catchup=False (don't backfill historical runs on first deploy)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

SPARK_JOBS = "/home/{{ var.value.roip_user }}/projects/roip"
VENV       = f"{SPARK_JOBS}/.venv/bin/python"

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,    # set True in production with SMTP config
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="roip_daily_pipeline",
    default_args=default_args,
    description="ROIP daily batch: Bronze→Silver→Gold + reconciliation",
    schedule_interval="0 2 * * *",        # 02:00 UTC every day
    start_date=days_ago(1),
    catchup=False,                        # don't rerun missed dates
    tags=["roip", "batch", "medallion"],
    doc_md="""
    ## ROIP Daily Pipeline

    Transforms Bronze raw events into Silver clean facts
    and Gold business aggregates. Runs reconciliation to
    detect data loss between layers.

    **SLA**: Must complete by 04:00 UTC.
    **Owner**: Data Engineering team.
    """,
) as dag:

    # ── Task 1: Bronze → Silver ───────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {SPARK_JOBS}/batch/jobs && "
            f"{VENV} bronze_to_silver.py "
            "--date {{ ds }}"        # ds = execution date (YYYY-MM-DD)
        ),
        doc_md="Transforms raw Bronze events into typed Silver facts.",
    )

    # ── Task 2: Silver → Gold ─────────────────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {SPARK_JOBS}/batch/jobs && "
            f"{VENV} silver_to_gold.py "
            "--date {{ ds }}"
        ),
        doc_md="Builds Gold aggregates for dashboards and SLA reports.",
    )

    # ── Task 3: Reconciliation ────────────────────────────────────────
    reconciliation = BashOperator(
        task_id="reconciliation",
        bash_command=(
            f"cd {SPARK_JOBS}/batch/reconciliation && "
            f"{VENV} daily_reconciliation.py "
            "--date {{ ds }}"
        ),
        doc_md="Validates Bronze vs Silver record counts. Fails if >1% variance.",
    )

    # ── Task 4: DQ gate — branch on reconciliation result ─────────────
    def check_dq_gate(**context):
        """
        Check if reconciliation passed.
        If yes → mark_success. If no → trigger_backfill.
        """
        import subprocess
        result = subprocess.run(
            ["grep", "-c", "PASS",
             f"/tmp/roip/lakehouse/monitoring/recon_{context['ds']}.log"],
            capture_output=True
        )
        if result.returncode == 0:
            return "pipeline_success"
        return "trigger_alert"

    dq_gate = BranchPythonOperator(
        task_id="dq_gate",
        python_callable=check_dq_gate,
    )

    pipeline_success = BashOperator(
        task_id="pipeline_success",
        bash_command='echo "Pipeline completed successfully for {{ ds }}"',
    )

    trigger_alert = BashOperator(
        task_id="trigger_alert",
        bash_command=(
            'echo "ALERT: Reconciliation failed for {{ ds }}. '
            'Manual review required." && exit 1'
        ),
    )

    # ── DAG wiring ────────────────────────────────────────────────────
    # bronze_to_silver must finish before silver_to_gold starts
    # reconciliation runs after both silver_to_gold completes
    bronze_to_silver >> silver_to_gold >> reconciliation >> dq_gate
    dq_gate >> [pipeline_success, trigger_alert]