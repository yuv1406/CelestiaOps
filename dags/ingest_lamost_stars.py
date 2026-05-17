"""
DAG: ingest_lamost_stars
Schedule: daily at 05:00 UTC  (runs after ingest_exoplanets finishes at ~02:00)

For every planet-hosting star in the exoplanets table, fetches matching
spectral observations from the LAMOST DR5 low-resolution survey catalog
via the VizieR TAP service (2-arcsec cone search) and upserts them into
a dedicated 'lamost' TimescaleDB database.

Task chain:
  ensure_lamost_db → ingest_lamost_data → log_stats

Setup required (one-time):
  Add an Airflow connection named 'celestiaops_lamost_postgres':
    Conn Type : Postgres
    Host      : celestiaops_timescaledb
    Schema    : lamost
    Login     : celestia
    Password  : celestia
    Port      : 5432
"""

import logging
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/airflow")
from include.config.lamost_settings import LAMOST_CONN_ID
from include.config.settings import POSTGRES_CONN_ID
from operators.lamost_to_postgres_operator import LamostToPostgresOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


def _ensure_lamost_db():
    """
    Idempotently creates the 'lamost' PostgreSQL database within the same
    TimescaleDB instance and initialises the schema.

    Uses autocommit=True for CREATE DATABASE (required by PostgreSQL) and
    derives all connection parameters from the existing celestiaops_postgres
    Airflow connection so no additional configuration is needed for this step.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_obj = hook.get_connection(POSTGRES_CONN_ID)

    host     = conn_obj.host
    port     = conn_obj.port or 5432
    user     = conn_obj.login
    password = conn_obj.password

    # --- Step 1: create 'lamost' database if it doesn't exist ---
    admin_conn = psycopg2.connect(
        host=host, port=port, dbname="exoplanets", user=user, password=password
    )
    admin_conn.autocommit = True
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'lamost'")
            if cur.fetchone():
                log.info("'lamost' database already exists — skipping creation")
            else:
                cur.execute("CREATE DATABASE lamost")
                log.info("Created 'lamost' database")
    finally:
        admin_conn.close()

    # --- Step 2: enable TimescaleDB extension and create tables ---
    lamost_conn = psycopg2.connect(
        host=host, port=port, dbname="lamost", user=user, password=password
    )
    try:
        with lamost_conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
        lamost_conn.commit()

        schema_sql = open("/opt/airflow/include/sql/create_lamost_tables.sql").read()
        with lamost_conn.cursor() as cur:
            cur.execute(schema_sql)
        lamost_conn.commit()
        log.info("LAMOST schema initialised")
    finally:
        lamost_conn.close()


with DAG(
    dag_id="ingest_lamost_stars",
    description="Daily ingestion of LAMOST spectral data for exoplanet host stars",
    schedule="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "ingest", "lamost"],
    doc_md=__doc__,
) as dag:

    ensure_lamost_db = PythonOperator(
        task_id="ensure_lamost_db",
        python_callable=_ensure_lamost_db,
    )

    ingest = LamostToPostgresOperator(
        task_id="ingest_lamost_data",
        src_conn_id=POSTGRES_CONN_ID,
        lamost_conn_id=LAMOST_CONN_ID,
        execution_timeout=timedelta(hours=2),
    )

    def _log_stats(**context):
        stats = context["ti"].xcom_pull(task_ids="ingest_lamost_data")
        if stats:
            log.info(
                "[ingest_lamost_stars] run_id=%s stars_queried=%d inserted=%d updated=%d",
                context["run_id"],
                stats.get("stars_queried", 0),
                stats.get("total_inserted", 0),
                stats.get("total_updated", 0),
            )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=_log_stats,
    )

    ensure_lamost_db >> ingest >> log_stats
