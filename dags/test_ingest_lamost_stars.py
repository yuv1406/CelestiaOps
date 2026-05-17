"""
DAG: test_ingest_lamost_stars

Test version of ingest_lamost_stars that writes fetched LAMOST spectral data
to a CSV file in /opt/airflow/results/ instead of upserting into TimescaleDB.

Does NOT require the lamost database to exist — only the exoplanets DB is
needed to resolve host star coordinates.

Task chain:
  fetch_to_csv → log_stats
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")
from operators.lamost_to_csv_operator import LamostToCsvOperator

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="test_ingest_lamost_stars",
    description="[TEST] Fetch LAMOST spectral data for host stars → CSV in results/",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "test", "ingest", "lamost"],
    doc_md=__doc__,
) as dag:

    fetch = LamostToCsvOperator(
        task_id="fetch_to_csv",
        execution_timeout=timedelta(hours=2),
    )

    def _log_stats(**context):
        stats = context["ti"].xcom_pull(task_ids="fetch_to_csv")
        if stats:
            print(
                f"[test_ingest_lamost_stars] run_id={context['run_id']} "
                f"stars_queried={stats.get('stars_queried')} "
                f"stars_matched={stats.get('stars_matched')} "
                f"rows={stats.get('total_rows')} "
                f"output={stats.get('output_file')}"
            )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=_log_stats,
    )

    fetch >> log_stats
