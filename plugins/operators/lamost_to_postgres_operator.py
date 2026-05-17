import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

sys.path.insert(0, "/opt/airflow")
from include.config.lamost_settings import (
    LAMOST_CHECKSUM_COLUMNS,
    LAMOST_CONE_RADIUS_DEG,
    LAMOST_COLUMNS,
    LAMOST_CONN_ID,
    LAMOST_MAX_OBS_PER_STAR,
    LAMOST_QUERY_DELAY_SEC,
    LAMOST_TABLE,
    VIZIER_TAP_URL,
)
from include.config.settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

# Build the SELECT clause: LAMOST_COLUMNS contains pre-quoted entries like
# '"[Fe/H]"' for ADQL identifiers with special characters.
_SELECT_COLS = ", ".join(LAMOST_COLUMNS)

# ADQL cone-search query template for VizieR TAP.
# TOP limits per-star results; ORDER BY snrg DESC picks the best spectra.
_ADQL_TEMPLATE = (
    "SELECT TOP {limit} {cols} "
    "FROM {table} "
    "WHERE CONTAINS("
    "  POINT('ICRS', RAJ2000, DEJ2000),"
    "  CIRCLE('ICRS', {ra}, {dec}, {radius})"
    ") = 1 "
    "ORDER BY snrg DESC"
)


def _compute_checksum(row: dict) -> str:
    payload = {k: row.get(k) for k in LAMOST_CHECKSUM_COLUMNS}
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _coerce(value: Any) -> Any:
    """Return None for empty strings so postgres stores NULL."""
    if value == "" or value is None:
        return None
    return value


class LamostToPostgresOperator(BaseOperator):
    """
    For every unique host star in the exoplanets table, issues a VizieR TAP
    ADQL cone-search against the LAMOST DR5 stellar parameters catalog
    (V/164/stellar5, 5.3M rows), then upserts only changed or new observations
    into the lamost TimescaleDB database — skipping rows whose checksum matches.

    Coverage note: LAMOST targets V≈10–17.8 mag in the northern sky (dec > -10°).
    Bright famous host stars (51 Peg, 55 Cnc) are typically not in the catalog;
    Kepler/K2/TESS field stars are well-represented.
    """

    template_fields = ("src_conn_id", "lamost_conn_id")

    def __init__(
        self,
        src_conn_id: str = POSTGRES_CONN_ID,
        lamost_conn_id: str = LAMOST_CONN_ID,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.src_conn_id = src_conn_id
        self.lamost_conn_id = lamost_conn_id

    # ------------------------------------------------------------------
    # Fetch host stars from the exoplanets database
    # ------------------------------------------------------------------

    def _fetch_host_stars(self, hook: PostgresHook) -> list[tuple]:
        """Return distinct (hostname, ra, dec) rows from the exoplanets table."""
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT hostname, ra, dec
                    FROM exoplanets
                    WHERE hostname IS NOT NULL
                      AND ra IS NOT NULL
                      AND dec IS NOT NULL
                    ORDER BY hostname
                    """
                )
                rows = cur.fetchall()
        log.info("Found %d unique host stars with coordinates", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Query VizieR LAMOST catalog
    # ------------------------------------------------------------------

    def _query_vizier(self, ra: float, dec: float) -> list[dict]:
        """
        Run a VizieR TAP ADQL cone search and return rows as dicts.

        VizieR returns JSON in {"metadata": [...], "data": [[...]]} shape.
        Column names come from metadata — including the literal '[Fe/H]' and
        'e_[Fe/H]' strings (VizieR preserves them; ADQL quoting only affects
        the query syntax, not the response keys).
        """
        adql = _ADQL_TEMPLATE.format(
            limit=LAMOST_MAX_OBS_PER_STAR,
            cols=_SELECT_COLS,
            table=LAMOST_TABLE,
            ra=ra,
            dec=dec,
            radius=LAMOST_CONE_RADIUS_DEG,
        )
        resp = requests.get(
            VIZIER_TAP_URL,
            params={
                "REQUEST": "doQuery",
                "LANG": "ADQL",
                "FORMAT": "json",
                "QUERY": adql,
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()

        data_rows = payload.get("data") or []
        if not data_rows:
            return []

        col_names = [col["name"] for col in payload.get("metadata", [])]
        return [dict(zip(col_names, row)) for row in data_rows]

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def _prepare_rows(self, hostname: str, obs_list: list[dict]) -> list[tuple]:
        now = datetime.now(timezone.utc)
        prepared = []
        for row in obs_list:
            obsid = _coerce(row.get("ObsID"))
            if obsid is None:
                continue
            checksum = _compute_checksum(row)
            prepared.append((
                int(obsid),
                hostname,
                _coerce(row.get("RAJ2000")),
                _coerce(row.get("DEJ2000")),
                _coerce(row.get("Teff")),
                _coerce(row.get("e_Teff")),
                _coerce(row.get("logg")),
                _coerce(row.get("e_logg")),
                _coerce(row.get("[Fe/H]")),    # VizieR returns this key literally
                _coerce(row.get("e_[Fe/H]")),
                _coerce(row.get("HRV")),
                _coerce(row.get("e_HRV")),
                _coerce(row.get("snrg")),
                _coerce(row.get("snrr")),
                _coerce(row.get("Class")),
                _coerce(row.get("SubClass")),
                _coerce(row.get("ObsDate")),
                checksum,
                now,    # ingested_at
                now,    # updated_at
            ))
        return prepared

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def _upsert(self, hook: PostgresHook, rows: list[tuple]) -> dict:
        if not rows:
            return {"inserted": 0, "updated": 0}

        upsert_sql = open("/opt/airflow/include/sql/upsert_lamost.sql").read()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM lamost_observations")
                before = cur.fetchone()[0]
                execute_values(cur, upsert_sql, rows, page_size=500)
                conn.commit()
                cur.execute("SELECT COUNT(*) FROM lamost_observations")
                after = cur.fetchone()[0]

        inserted = max(0, after - before)
        updated = max(0, len(rows) - inserted)
        return {"inserted": inserted, "updated": updated}

    def _record_sync_state(
        self, hook: PostgresHook, dag_id: str, stars_queried: int, stats: dict
    ):
        hook.run(
            """
            INSERT INTO lamost_sync_state
                (dag_id, last_sync_at, stars_queried, obs_inserted, obs_updated)
            VALUES (%s, NOW(), %s, %s, %s)
            ON CONFLICT (dag_id) DO UPDATE SET
                last_sync_at  = EXCLUDED.last_sync_at,
                stars_queried = EXCLUDED.stars_queried,
                obs_inserted  = EXCLUDED.obs_inserted,
                obs_updated   = EXCLUDED.obs_updated
            """,
            parameters=(
                dag_id,
                stars_queried,
                stats["inserted"],
                stats["updated"],
            ),
        )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute(self, context):
        src_hook    = PostgresHook(postgres_conn_id=self.src_conn_id)
        lamost_hook = PostgresHook(postgres_conn_id=self.lamost_conn_id)

        host_stars = self._fetch_host_stars(src_hook)
        total_inserted = total_updated = 0

        for hostname, ra, dec in host_stars:
            try:
                obs_list = self._query_vizier(ra, dec)
                if obs_list:
                    rows  = self._prepare_rows(hostname, obs_list)
                    stats = self._upsert(lamost_hook, rows)
                    total_inserted += stats["inserted"]
                    total_updated  += stats["updated"]
                    log.info(
                        "Star %-30s  matched=%d  inserted=%d  updated=%d",
                        hostname, len(obs_list), stats["inserted"], stats["updated"],
                    )
                time.sleep(LAMOST_QUERY_DELAY_SEC)
            except Exception as exc:
                log.warning(
                    "LAMOST query failed for %s (ra=%.4f dec=%.4f): %s",
                    hostname, ra, dec, exc,
                )

        result = {
            "stars_queried":  len(host_stars),
            "total_inserted": total_inserted,
            "total_updated":  total_updated,
        }
        self._record_sync_state(
            lamost_hook,
            context["dag_run"].dag_id,
            len(host_stars),
            {"inserted": total_inserted, "updated": total_updated},
        )
        log.info(
            "LAMOST sync complete — stars=%d inserted=%d updated=%d",
            result["stars_queried"], result["total_inserted"], result["total_updated"],
        )
        return result
