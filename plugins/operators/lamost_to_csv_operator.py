"""
LamostToCsvOperator — test variant of LamostToPostgresOperator.

For every planet-hosting star in the exoplanets table, fetches matching
spectral observations from the LAMOST DR5 catalog via VizieR TAP and writes
all results to a CSV file under /opt/airflow/results/ instead of upserting
into TimescaleDB.

Output filename: lamost_<run_id>.csv
"""

import csv
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/airflow")
from include.config.lamost_settings import (
    LAMOST_CHECKSUM_COLUMNS,
    LAMOST_CONE_RADIUS_DEG,
    LAMOST_COLUMNS,
    LAMOST_MAX_OBS_PER_STAR,
    LAMOST_QUERY_DELAY_SEC,
    LAMOST_TABLE,
    VIZIER_TAP_URL,
)
from include.config.settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

RESULTS_DIR = "/opt/airflow/results"

_SELECT_COLS = ", ".join(LAMOST_COLUMNS)

_ADQL_TEMPLATE = (
    "SELECT TOP {limit} {cols} "
    "FROM {table} "
    "WHERE CONTAINS("
    "  POINT('ICRS', RAJ2000, DEJ2000),"
    "  CIRCLE('ICRS', {ra}, {dec}, {radius})"
    ") = 1 "
    "ORDER BY snrg DESC"
)

CSV_COLUMNS = [
    "obsid", "hostname", "ra", "dec",
    "teff", "e_teff", "logg", "e_logg",
    "feh", "e_feh", "hrv", "e_hrv",
    "snr_g", "snr_r", "spec_class", "spec_subclass",
    "obs_date", "row_checksum", "ingested_at",
]


def _compute_checksum(row: dict) -> str:
    payload = {k: row.get(k) for k in LAMOST_CHECKSUM_COLUMNS}
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _coerce(value: Any) -> Any:
    """Return None for empty strings so CSV stores empty cell."""
    if value == "" or value is None:
        return None
    return value


class LamostToCsvOperator(BaseOperator):
    """
    Fetches LAMOST spectral data for every exoplanet host star and writes
    the result to a CSV file in /opt/airflow/results/.

    Skips all database writes — intended for validating fetch + transform
    logic before running against the live lamost TimescaleDB database.
    """

    template_fields = ("src_conn_id",)

    def __init__(self, src_conn_id: str = POSTGRES_CONN_ID, **kwargs):
        super().__init__(**kwargs)
        self.src_conn_id = src_conn_id

    def _fetch_host_stars(self, hook: PostgresHook) -> list[tuple]:
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

    def _query_vizier(self, ra: float, dec: float) -> list[dict]:
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

    def _to_csv_row(self, hostname: str, row: dict, now: str) -> dict:
        obsid = _coerce(row.get("ObsID"))
        if obsid is None:
            return None
        return {
            "obsid":        int(obsid),
            "hostname":     hostname,
            "ra":           _coerce(row.get("RAJ2000")),
            "dec":          _coerce(row.get("DEJ2000")),
            "teff":         _coerce(row.get("Teff")),
            "e_teff":       _coerce(row.get("e_Teff")),
            "logg":         _coerce(row.get("logg")),
            "e_logg":       _coerce(row.get("e_logg")),
            "feh":          _coerce(row.get("[Fe/H]")),
            "e_feh":        _coerce(row.get("e_[Fe/H]")),
            "hrv":          _coerce(row.get("HRV")),
            "e_hrv":        _coerce(row.get("e_HRV")),
            "snr_g":        _coerce(row.get("snrg")),
            "snr_r":        _coerce(row.get("snrr")),
            "spec_class":   _coerce(row.get("Class")),
            "spec_subclass": _coerce(row.get("SubClass")),
            "obs_date":     _coerce(row.get("ObsDate")),
            "row_checksum": _compute_checksum(row),
            "ingested_at":  now,
        }

    def execute(self, context):
        src_hook = PostgresHook(postgres_conn_id=self.src_conn_id)
        host_stars = self._fetch_host_stars(src_hook)

        now = datetime.now(timezone.utc).isoformat()
        safe_run_id = context["run_id"].replace(":", "-").replace("+", "").replace(" ", "_")

        os.makedirs(RESULTS_DIR, exist_ok=True)
        out_path = os.path.join(RESULTS_DIR, f"lamost_{safe_run_id}.csv")

        total_rows = 0
        stars_matched = 0

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

            for hostname, ra, dec in host_stars:
                try:
                    obs_list = self._query_vizier(ra, dec)
                    if obs_list:
                        stars_matched += 1
                        for obs in obs_list:
                            csv_row = self._to_csv_row(hostname, obs, now)
                            if csv_row:
                                writer.writerow(csv_row)
                                total_rows += 1
                        log.info(
                            "Star %-30s  matched=%d", hostname, len(obs_list)
                        )
                    time.sleep(LAMOST_QUERY_DELAY_SEC)
                except Exception as exc:
                    log.warning(
                        "LAMOST query failed for %s (ra=%.4f dec=%.4f): %s",
                        hostname, ra, dec, exc,
                    )

        log.info(
            "CSV written: %s — stars_queried=%d stars_matched=%d rows=%d",
            out_path, len(host_stars), stars_matched, total_rows,
        )
        return {
            "stars_queried": len(host_stars),
            "stars_matched": stars_matched,
            "total_rows": total_rows,
            "output_file": out_path,
        }
