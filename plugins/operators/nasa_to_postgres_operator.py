import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from include.config.settings import (
    CHECKSUM_COLUMNS,
    HZ_TEMP_MAX,
    HZ_TEMP_MIN,
    NASA_COLUMNS,
    NASA_FETCH_CHUNK,
    NASA_TABLE,
    NASA_TAP_BASE_URL,
    POSTGRES_CONN_ID,
)

log = logging.getLogger(__name__)


def _compute_checksum(row: dict) -> str:
    payload = {k: row.get(k) for k in CHECKSUM_COLUMNS}
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _coerce(value: Any) -> Any:
    """Return None for empty strings so postgres stores NULL."""
    if value == "" or value is None:
        return None
    return value


class NasaToPostgresOperator(BaseOperator):
    """
    Fetches exoplanet data from the NASA TAP API in paginated chunks,
    computes per-row checksums, then upserts only changed rows into
    TimescaleDB — skipping rows whose checksum already matches.
    """

    template_fields = ("postgres_conn_id",)

    def __init__(self, postgres_conn_id: str = POSTGRES_CONN_ID, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch_all_planets(self) -> list[dict]:
        cols = ",".join(NASA_COLUMNS)
        all_rows: list[dict] = []
        offset = 0

        while True:
            params = {
                "QUERY": f"SELECT {cols} FROM {NASA_TABLE}",
                "FORMAT": "json",
                "TOP": NASA_FETCH_CHUNK,
                "OFFSET": offset,
            }
            log.info("Fetching NASA TAP rows offset=%d limit=%d", offset, NASA_FETCH_CHUNK)
            resp = requests.get(NASA_TAP_BASE_URL, params=params, timeout=120)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            all_rows.extend(batch)
            log.info("Fetched %d rows (total so far: %d)", len(batch), len(all_rows))
            if len(batch) < NASA_FETCH_CHUNK:
                break
            offset += NASA_FETCH_CHUNK

        return all_rows

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def _prepare_rows(self, raw_rows: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc)
        prepared = []
        for row in raw_rows:
            checksum = _compute_checksum(row)
            eqt = _coerce(row.get("pl_eqt"))
            is_habitable = eqt is not None and HZ_TEMP_MIN <= eqt <= HZ_TEMP_MAX
            prepared.append((
                row.get("pl_name"),
                _coerce(row.get("hostname")),
                _coerce(row.get("sy_snum")),
                _coerce(row.get("sy_pnum")),
                _coerce(row.get("discoverymethod")),
                _coerce(row.get("disc_year")),
                _coerce(row.get("pl_orbper")),
                _coerce(row.get("pl_rade")),
                _coerce(row.get("pl_masse")),
                _coerce(row.get("pl_dens")),
                eqt,
                _coerce(row.get("pl_orbeccen")),
                _coerce(row.get("pl_orbsmax")),
                _coerce(row.get("pl_insol")),
                _coerce(row.get("st_teff")),
                _coerce(row.get("st_rad")),
                _coerce(row.get("st_mass")),
                _coerce(row.get("st_met")),
                _coerce(row.get("st_logg")),
                _coerce(row.get("sy_dist")),
                _coerce(row.get("sy_vmag")),
                _coerce(row.get("ra")),
                _coerce(row.get("dec")),
                checksum,
                is_habitable,
                now,  # ingested_at
                now,  # updated_at
            ))
        return prepared

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def _upsert(self, hook: PostgresHook, rows: list[tuple]) -> dict:
        upsert_sql = open("/opt/airflow/include/sql/upsert_exoplanets.sql").read()

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                # Capture row counts before and after to compute stats
                cur.execute("SELECT COUNT(*) FROM exoplanets")
                before = cur.fetchone()[0]

                execute_values(cur, upsert_sql, rows, page_size=500)
                conn.commit()

                cur.execute("SELECT COUNT(*) FROM exoplanets")
                after = cur.fetchone()[0]

        inserted = max(0, after - before)
        updated = sum(1 for _ in rows) - inserted  # rough; ON CONFLICT skips unchanged
        return {"inserted": inserted, "updated": updated, "total_fetched": len(rows)}

    def _record_sync_state(self, hook: PostgresHook, stats: dict, dag_id: str):
        hook.run(
            """
            INSERT INTO sync_state (dag_id, last_sync_at, rows_fetched, rows_inserted, rows_updated)
            VALUES (%s, NOW(), %s, %s, %s)
            ON CONFLICT (dag_id) DO UPDATE SET
                last_sync_at  = EXCLUDED.last_sync_at,
                rows_fetched  = EXCLUDED.rows_fetched,
                rows_inserted = EXCLUDED.rows_inserted,
                rows_updated  = EXCLUDED.rows_updated
            """,
            parameters=(
                dag_id,
                stats["total_fetched"],
                stats["inserted"],
                stats["updated"],
            ),
        )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        raw = self._fetch_all_planets()
        log.info("Total planets fetched from NASA: %d", len(raw))

        rows = self._prepare_rows(raw)
        stats = self._upsert(hook, rows)
        self._record_sync_state(hook, stats, context["dag_run"].dag_id)

        log.info(
            "Sync complete — fetched=%d inserted=%d updated=%d",
            stats["total_fetched"],
            stats["inserted"],
            stats["updated"],
        )
        return stats
