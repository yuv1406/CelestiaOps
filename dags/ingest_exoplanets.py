"""
DAG: ingest_exoplanets
Schedule: daily at 02:00 UTC

Pulls the full Planetary Systems Composite Parameters table from the
NASA Exoplanet Archive TAP service, computes SHA-256 checksums per row,
and upserts only changed or new records into TimescaleDB.

Task chain:
  ensure_schema → ingest_nasa_data → notify_downstream
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/airflow")
from include.config.settings import POSTGRES_CONN_ID
from operators.nasa_to_postgres_operator import NasaToPostgresOperator

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="ingest_exoplanets",
    description="Daily ingestion of NASA exoplanet data into TimescaleDB",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "ingest", "nasa"],
    doc_md=__doc__,
) as dag:

    def _ensure_schema():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = open("/opt/airflow/include/sql/create_tables.sql").read()
        hook.run(sql)

    ensure_schema = PythonOperator(
        task_id="ensure_schema",
        python_callable=_ensure_schema,
    )

    ingest = NasaToPostgresOperator(
        task_id="ingest_nasa_data",
        postgres_conn_id=POSTGRES_CONN_ID,
        execution_timeout=timedelta(minutes=30),
    )

    def _log_stats(**context):
        stats = context["ti"].xcom_pull(task_ids="ingest_nasa_data")
        if stats:
            print(
                f"[ingest_exoplanets] run_id={context['run_id']} "
                f"fetched={stats.get('total_fetched')} "
                f"inserted={stats.get('inserted')} "
                f"updated={stats.get('updated')}"
            )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=_log_stats,
    )

    ensure_schema >> ingest >> log_stats
