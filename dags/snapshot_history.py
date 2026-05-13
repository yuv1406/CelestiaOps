"""
DAG: snapshot_history
Schedule: every Sunday at 06:00 UTC

Copies the current state of the exoplanets table into the
exoplanets_history hypertable, tagged with the current timestamp.
This provides weekly point-in-time snapshots that power trend queries
(e.g. "how many new planets were confirmed this year?").

Task chain:
  check_data_freshness → create_snapshot → prune_old_snapshots
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.config.settings import POSTGRES_CONN_ID

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SNAPSHOT_RETENTION_YEARS = 5


def check_data_freshness(**context):
    """
    Verifies that the exoplanets table was synced recently enough before
    creating a snapshot. Fails the task if data is stale (>36 hours old).
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first("SELECT last_sync_at FROM sync_state WHERE dag_id = 'ingest_exoplanets'")

    if not result:
        raise ValueError("No sync_state record found for ingest_exoplanets — run ingest first")

    last_sync = result[0]
    age = datetime.now(timezone.utc) - last_sync
    if age > timedelta(hours=36):
        raise ValueError(
            f"Exoplanet data is stale ({age} since last sync). "
            "Refusing to snapshot outdated data."
        )

    print(f"Data freshness OK — last synced {age} ago")


def create_snapshot(**context):
    """Inserts a full snapshot of the current exoplanets table into history."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(
        """
        INSERT INTO exoplanets_history (
            snapshot_time, pl_name, hostname, disc_year,
            pl_orbper, pl_rade, pl_masse, pl_eqt,
            st_teff, st_rad, st_mass, sy_dist, row_checksum
        )
        SELECT
            NOW() AS snapshot_time,
            pl_name, hostname, disc_year,
            pl_orbper, pl_rade, pl_masse, pl_eqt,
            st_teff, st_rad, st_mass, sy_dist, row_checksum
        FROM exoplanets
        ON CONFLICT (snapshot_time, pl_name) DO NOTHING
        """
    )

    count = hook.get_first("SELECT COUNT(*) FROM exoplanets")[0]
    print(f"Snapshot created — {count} planets recorded at this point in time")
    context["ti"].xcom_push(key="snapshot_count", value=count)


def prune_old_snapshots(**context):
    """
    Removes snapshot rows older than SNAPSHOT_RETENTION_YEARS.
    TimescaleDB chunk management handles the physical deletion efficiently.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        DELETE FROM exoplanets_history
        WHERE snapshot_time < NOW() - INTERVAL '{SNAPSHOT_RETENTION_YEARS} years'
        """
    )
    print(f"Pruned snapshots older than {SNAPSHOT_RETENTION_YEARS} years")


with DAG(
    dag_id="snapshot_history",
    description="Weekly point-in-time snapshots of exoplanet catalog into TimescaleDB",
    schedule="0 6 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "snapshot", "history"],
    doc_md=__doc__,
) as dag:

    t_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    t_snapshot = PythonOperator(
        task_id="create_snapshot",
        python_callable=create_snapshot,
        execution_timeout=timedelta(minutes=10),
    )

    t_prune = PythonOperator(
        task_id="prune_old_snapshots",
        python_callable=prune_old_snapshots,
    )

    t_check >> t_snapshot >> t_prune
