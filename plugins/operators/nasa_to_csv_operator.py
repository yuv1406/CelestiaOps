"""
NasaToCsvOperator — test variant of NasaToPostgresOperator.

Fetches exoplanet data from the NASA TAP API and writes it to a CSV file
under /opt/airflow/results/ instead of upserting into TimescaleDB.
Useful for validating fetch + transform logic without a live database.

Output filename: exoplanets_<run_id>.csv
"""

import csv
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

from airflow.models import BaseOperator

from include.config.settings import (
    CHECKSUM_COLUMNS,
    HZ_TEMP_MAX,
    HZ_TEMP_MIN,
    NASA_COLUMNS,
    NASA_FETCH_CHUNK,
    NASA_TABLE,
    NASA_TAP_BASE_URL,
)

log = logging.getLogger(__name__)

RESULTS_DIR = "/opt/airflow/results"

CSV_COLUMNS = NASA_COLUMNS + ["row_checksum", "is_habitable_zone", "ingested_at"]


def _compute_checksum(row: dict) -> str:
    payload = {k: row.get(k) for k in CHECKSUM_COLUMNS}
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _coerce(value: Any) -> Any:
    if value == "" or value is None:
        return None
    return value


class NasaToCsvOperator(BaseOperator):
    """
    Fetches exoplanet data from the NASA TAP API and writes the result to
    a CSV file in /opt/airflow/results/ (mounted from the host results/ dir).

    Skips all database interactions — intended for pipeline testing only.
    """

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

    def _write_csv(self, raw_rows: list[dict], run_id: str) -> str:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        # Sanitize run_id for use in filename
        safe_run_id = run_id.replace(":", "-").replace("+", "").replace(" ", "_")
        out_path = os.path.join(RESULTS_DIR, f"exoplanets_{safe_run_id}.csv")

        now = datetime.now(timezone.utc).isoformat()

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in raw_rows:
                checksum = _compute_checksum(row)
                eqt = _coerce(row.get("pl_eqt"))
                is_habitable = eqt is not None and HZ_TEMP_MIN <= eqt <= HZ_TEMP_MAX
                writer.writerow({
                    **{col: _coerce(row.get(col)) for col in NASA_COLUMNS},
                    "row_checksum": checksum,
                    "is_habitable_zone": is_habitable,
                    "ingested_at": now,
                })

        return out_path

    def execute(self, context):
        raw = self._fetch_all_planets()
        log.info("Total planets fetched from NASA: %d", len(raw))

        run_id = context["run_id"]
        out_path = self._write_csv(raw, run_id)

        log.info("Written %d rows to %s", len(raw), out_path)
        return {"total_fetched": len(raw), "output_file": out_path}
