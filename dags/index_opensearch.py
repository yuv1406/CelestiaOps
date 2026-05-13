"""
DAG: index_opensearch
Schedule: daily at 04:00 UTC (after ingest_exoplanets)

Reads all exoplanet rows updated in the last 25 hours from TimescaleDB
and bulk-indexes them into OpenSearch. On first run (or after a full
re-index is triggered), indexes the entire table.

Task chain:
  get_changed_rows → bulk_index → update_alias
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.config.settings import (
    OPENSEARCH_CONN_ID,
    OPENSEARCH_INDEX,
    POSTGRES_CONN_ID,
)

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "celestiaops",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _get_opensearch_client():
    from airflow.hooks.base import BaseHook
    from opensearchpy import OpenSearch

    conn = BaseHook.get_connection(OPENSEARCH_CONN_ID)
    host = conn.host or "localhost"
    port = conn.port or 9200
    scheme = conn.schema or "http"
    return OpenSearch(
        hosts=[{"host": host, "port": port, "scheme": scheme}],
        http_auth=(conn.login, conn.password) if conn.login else None,
        timeout=30,
    )


def _is_potentially_habitable(row: dict) -> bool:
    eqt = row.get("pl_eqt")
    return eqt is not None and 180 <= eqt <= 310


# ------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------

def fetch_changed_rows(**context) -> int:
    """
    Queries TimescaleDB for rows updated in the past 25 hours and pushes
    them to XCom as a JSON string. Returns the row count.
    """
    force_reindex = Variable.get("opensearch_force_reindex", default_var="false") == "true"
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    if force_reindex:
        log.info("Force re-index requested — fetching all rows")
        sql = "SELECT * FROM exoplanets ORDER BY pl_name"
        params = None
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=25)
        log.info("Fetching rows updated since %s", cutoff.isoformat())
        sql = "SELECT * FROM exoplanets WHERE updated_at >= %s ORDER BY updated_at DESC"
        params = (cutoff,)

    rows = hook.get_records(sql, parameters=params)
    cols = [
        "pl_name", "hostname", "sy_snum", "sy_pnum", "discoverymethod", "disc_year",
        "pl_orbper", "pl_rade", "pl_masse", "pl_dens", "pl_eqt", "pl_orbeccen",
        "pl_orbsmax", "pl_insol", "st_teff", "st_rad", "st_mass", "st_met", "st_logg",
        "sy_dist", "sy_vmag", "ra", "dec", "row_checksum", "ingested_at", "updated_at",
    ]
    dicts = []
    for row in rows:
        d = dict(zip(cols, row))
        # Serialize timestamps for JSON
        d["ingested_at"] = d["ingested_at"].isoformat() if d["ingested_at"] else None
        d["updated_at"] = d["updated_at"].isoformat() if d["updated_at"] else None
        d["is_potentially_habitable"] = _is_potentially_habitable(d)
        dicts.append(d)

    log.info("Rows to index: %d", len(dicts))
    context["ti"].xcom_push(key="rows_json", value=json.dumps(dicts))
    return len(dicts)


def bulk_index_to_opensearch(**context):
    """Bulk-indexes changed rows into OpenSearch."""
    rows_json = context["ti"].xcom_pull(task_ids="fetch_changed_rows", key="rows_json")
    if not rows_json:
        log.info("No rows to index — skipping")
        return

    rows = json.loads(rows_json)
    if not rows:
        log.info("Empty row set — skipping")
        return

    client = _get_opensearch_client()

    # Ensure index exists with basic mapping
    if not client.indices.exists(OPENSEARCH_INDEX):
        client.indices.create(
            OPENSEARCH_INDEX,
            body={
                "settings": {"number_of_shards": 2, "number_of_replicas": 1},
                "mappings": {
                    "properties": {
                        "pl_name": {"type": "keyword"},
                        "hostname": {"type": "keyword"},
                        "discoverymethod": {"type": "keyword"},
                        "disc_year": {"type": "integer"},
                        "pl_rade": {"type": "float"},
                        "pl_masse": {"type": "float"},
                        "pl_eqt": {"type": "float"},
                        "sy_dist": {"type": "float"},
                        "is_potentially_habitable": {"type": "boolean"},
                        "updated_at": {"type": "date"},
                        "location": {"type": "geo_point"},
                    }
                },
            },
        )
        log.info("Created OpenSearch index: %s", OPENSEARCH_INDEX)

    # Build bulk body
    bulk_body = []
    for row in rows:
        bulk_body.append({"index": {"_index": OPENSEARCH_INDEX, "_id": row["pl_name"]}})
        doc = dict(row)
        # Enrich with geo_point if coordinates available
        if doc.get("ra") is not None and doc.get("dec") is not None:
            doc["location"] = {"lat": doc["dec"], "lon": doc["ra"]}
        bulk_body.append(doc)

    resp = client.bulk(body=bulk_body, refresh=False)
    if resp.get("errors"):
        error_items = [i for i in resp["items"] if i.get("index", {}).get("error")]
        log.warning("%d documents failed to index", len(error_items))
        for item in error_items[:5]:
            log.warning("Index error: %s", item)
    else:
        log.info("Successfully indexed %d documents", len(rows))

    # Reset force re-index flag if it was set
    if Variable.get("opensearch_force_reindex", default_var="false") == "true":
        Variable.set("opensearch_force_reindex", "false")


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="index_opensearch",
    description="Daily incremental sync of exoplanet data to OpenSearch",
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["celestiaops", "opensearch", "index"],
    doc_md=__doc__,
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_changed_rows",
        python_callable=fetch_changed_rows,
    )

    t_index = PythonOperator(
        task_id="bulk_index_to_opensearch",
        python_callable=bulk_index_to_opensearch,
        execution_timeout=timedelta(minutes=15),
    )

    t_fetch >> t_index
