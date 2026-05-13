"""
DAG: test_snapshot_history

Test version of snapshot_history. Instead of reading from TimescaleDB and
writing to exoplanets_history, it reads from the most recent exoplanets CSV
in results/ and writes a timestamped snapshot CSV.

Task chain:
  check_csv_freshness → create_snapshot_csv → log_stats
"""

import csv
import glob
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

RESULTS_DIR = "/opt/airflow/results"
SNAPSHOT_COLUMNS = [
    "snapshot_time", "pl_name", "hostname", "disc_year",
    "pl_orbper", "pl_rade", "pl_masse", "pl_eqt",
    "st_teff", "st_rad", "st_mass", "sy_dist", "row_checksum",
]
FRESHNESS_THRESHOLD = timedelta(hours=1)  # tighter threshold for test runs

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _latest_ingest_csv() -> str | None:
    matches = sorted(glob.glob(os.path.join(RESULTS_DIR, "exoplanets_*.csv")))
    # Exclude snapshot files
    matches = [p for p in matches if "snapshot" not in os.path.basename(p)]
    return matches[-1] if matches else None


def check_csv_freshness(**context):
    path = _latest_ingest_csv()
    if not path:
        raise FileNotFoundError(
            f"No exoplanets_*.csv found in {RESULTS_DIR} — run test_ingest_exoplanets first"
        )

    mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
    age = datetime.now(timezone.utc) - mtime
    if age > FRESHNESS_THRESHOLD:
        raise ValueError(
            f"Ingest CSV is stale ({age} old): {path}\n"
            "Re-run test_ingest_exoplanets to refresh."
        )

    print(f"CSV freshness OK — {os.path.basename(path)} written {age} ago")
    context["ti"].xcom_push(key="ingest_csv", value=path)


def create_snapshot_csv(**context):
    ingest_csv = context["ti"].xcom_pull(task_ids="check_csv_freshness", key="ingest_csv")
    snapshot_time = datetime.now(timezone.utc).isoformat()
    safe_ts = snapshot_time.replace(":", "-").replace("+", "")
    out_path = os.path.join(RESULTS_DIR, f"snapshot_{safe_ts}.csv")

    with open(ingest_csv) as src, open(out_path, "w", newline="") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=SNAPSHOT_COLUMNS)
        writer.writeheader()
        count = 0
        for row in reader:
            writer.writerow({
                "snapshot_time": snapshot_time,
                "pl_name": row["pl_name"],
                "hostname": row["hostname"],
                "disc_year": row["disc_year"],
                "pl_orbper": row["pl_orbper"],
                "pl_rade": row["pl_rade"],
                "pl_masse": row["pl_masse"],
                "pl_eqt": row["pl_eqt"],
                "st_teff": row["st_teff"],
                "st_rad": row["st_rad"],
                "st_mass": row["st_mass"],
                "sy_dist": row["sy_dist"],
                "row_checksum": row["row_checksum"],
            })
            count += 1

    print(f"Snapshot written — {count} planets at {snapshot_time}")
    print(f"Output: {out_path}")
    context["ti"].xcom_push(key="snapshot_count", value=count)
    context["ti"].xcom_push(key="snapshot_file", value=out_path)


def log_stats(**context):
    count = context["ti"].xcom_pull(task_ids="create_snapshot_csv", key="snapshot_count")
    out_path = context["ti"].xcom_pull(task_ids="create_snapshot_csv", key="snapshot_file")
    print(
        f"[test_snapshot_history] run_id={context['run_id']} "
        f"snapshot_count={count} output={out_path}"
    )


with DAG(
    dag_id="test_snapshot_history",
    description="[TEST] Snapshot exoplanet CSV → timestamped snapshot CSV in results/",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "test", "snapshot", "history"],
    doc_md=__doc__,
) as dag:

    t_check = PythonOperator(
        task_id="check_csv_freshness",
        python_callable=check_csv_freshness,
    )

    t_snapshot = PythonOperator(
        task_id="create_snapshot_csv",
        python_callable=create_snapshot_csv,
        execution_timeout=timedelta(minutes=5),
    )

    t_log = PythonOperator(
        task_id="log_stats",
        python_callable=log_stats,
    )

    t_check >> t_snapshot >> t_log
