"""
DAG: test_ingest_exoplanets
Branch: test/file-output

Test version of ingest_exoplanets that writes fetched data to a CSV file
in /opt/airflow/results/ instead of upserting into TimescaleDB.

Task chain:
  fetch_to_csv → log_stats
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.operators.nasa_to_csv_operator import NasaToCsvOperator

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="test_ingest_exoplanets",
    description="[TEST] Fetch NASA exoplanet data → CSV in results/",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "test", "ingest", "nasa"],
    doc_md=__doc__,
) as dag:

    fetch = NasaToCsvOperator(
        task_id="fetch_to_csv",
        execution_timeout=timedelta(minutes=30),
    )

    def _log_stats(**context):
        stats = context["ti"].xcom_pull(task_ids="fetch_to_csv")
        if stats:
            print(
                f"[test_ingest_exoplanets] run_id={context['run_id']} "
                f"fetched={stats.get('total_fetched')} "
                f"output={stats.get('output_file')}"
            )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=_log_stats,
    )

    fetch >> log_stats
