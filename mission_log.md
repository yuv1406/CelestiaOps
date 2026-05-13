# CelestiaOps Mission Log

Significant decisions, architecture choices, and milestones. Not a changelog — only things worth remembering *why*.

---

## 2026-05-13 — Project Kickoff

Reviewed full codebase. Confirmed core architecture:
- 3 DAGs: `ingest_exoplanets` (daily), `index_opensearch` (daily), `snapshot_history` (weekly)
- Custom `NasaToPostgresOperator` with paginated NASA TAP fetch + SHA-256 checksum-based upsert
- TimescaleDB hypertable for historical snapshots, 5-year retention
- Airflow 3.2.0 running on `airflow-stack_default` Docker network

---

## 2026-05-13 — Database Stack: docker-compose.yml Created

**Decision:** Created `CelestiaOps/docker-compose.yml` with TimescaleDB, OpenSearch, and Grafana.

**Why:** Services need to share the `airflow-stack_default` Docker network so Airflow workers can reach them by container name (e.g. `celestiaops_timescaledb:5432`). Kept as a separate compose file to avoid modifying the running Airflow stack.

---

## 2026-05-13 — Storage: Volumes Moved to /data Partition

**Decision:** Replaced Docker named volumes with bind mounts under `/data/celestiaops/`.

**Why:** Root filesystem (`/dev/sda2`) was at 80% usage with only 3.9 GB free — not enough headroom for OpenSearch data and image pulls. `/data` partition (`/dev/sda3`) has 191 GB free.

Directories: `/data/celestiaops/{timescaledb,opensearch,grafana}`

---

## 2026-05-13 — Bug: Three Import and Timezone Fixes Across DAGs

**Bugs fixed:**

1. **`ingest_exoplanets.py` — wrong operator import path:** `from plugins.operators...` fails because Airflow adds `plugins/` itself to `sys.path`, making `plugins` not a package. Fixed to `from operators.nasa_to_postgres_operator import ...`

2. **`ingest_exoplanets.py` + `snapshot_history.py` — `include` not on path:** `from include.config.settings import ...` fails at runtime since `/opt/airflow` is not on `sys.path` by default. Fixed with `sys.path.insert(0, "/opt/airflow")` before the import in both DAGs.

3. **`snapshot_history.py` — timezone-naive datetime comparison:** `datetime.now(timezone.utc) - last_sync` throws `TypeError` if PostgreSQL returns a naive datetime (no tzinfo). Fixed by coercing `last_sync` to UTC if naive.

4. **`nasa_to_csv_operator.py` — test fetch limit:** Now fetches 500 rows via `SELECT TOP 500` in the ADQL query (TAP respects TOP in the query string, not as a URL param). Renamed `NASA_FETCH_CHUNK` → `NASA_FETCH_LIMIT = 500`.

---

## 2026-05-13 — Bug: NASA TAP Pagination Loop Was Infinite

**Decision:** Removed pagination loop from `_fetch_all_planets` in both `NasaToPostgresOperator` and `NasaToCsvOperator`. Now fetches all rows in a single request.

**Why:** The NASA TAP API ignores `TOP` and `OFFSET` as URL parameters — it returns the full dataset (6,286 rows) on every request regardless. The break condition `len(batch) < NASA_FETCH_CHUNK` (5000) was never true since 6,286 > 5,000, causing an infinite fetch loop. Caught during test DAG run on `test/file-output` branch.

**What changed:**
- `plugins/operators/nasa_to_postgres_operator.py` — replaced pagination `while` loop with a single `requests.get`
- `plugins/operators/nasa_to_csv_operator.py` — same fix
- `NASA_FETCH_CHUNK` constant in `settings.py` is now unused (left in place for now)

---

## 2026-05-13 — Testing: File-Output Branch for DAG Validation Without DB

**Decision:** Created `test/file-output` branch with `NasaToCsvOperator` and `test_ingest_exoplanets` DAG that writes NASA fetch results to `/opt/airflow/results/` as CSV instead of upserting into TimescaleDB.

**Why:** Needed a way to validate the full fetch → transform pipeline (pagination, checksums, habitability flag) without requiring a live TimescaleDB connection. CSV output lands at `airflow-stack/results/exoplanets_<run_id>.csv` on the host via the existing volume mount.

**What changed:**
- `CelestiaOps/plugins/operators/nasa_to_csv_operator.py` — self-contained operator, no `include` imports; settings inlined to avoid Airflow plugin loader path issues
- `CelestiaOps/dags/test_ingest_exoplanets.py` — manual-trigger only (`schedule=None`), chain: `fetch_to_csv → log_stats`
- Copied operator + DAG into main `airflow-stack/` (`plugins/operators/`, `dags/`) so the main Airflow instance picks them up
- Airflow plugin path: import as `from operators.nasa_to_csv_operator import ...` (not `plugins.operators.*`) since Airflow adds `plugins/` itself to `sys.path`

---

## 2026-05-13 — Search Engine: OpenSearch Replaced with PostgreSQL FTS

**Decision:** Dropped OpenSearch entirely. Replaced with a GIN full-text search index on TimescaleDB. Deleted `dags/index_opensearch.py`.

**Why:** OpenSearch requires ~700 MB–1 GB RAM for its JVM alone. The exoplanet dataset is structured — queries are range filters (mass, temperature, year) plus keyword lookup on planet/star names. PostgreSQL FTS with a GIN index covers all of this natively with zero extra containers or RAM. The `index_opensearch` DAG was a sync layer that now has no purpose.

**What changed:**
- `create_tables.sql` — added `is_potentially_habitable BOOLEAN` column, partial index on it, GIN FTS index on `pl_name + hostname + discoverymethod`
- `upsert_exoplanets.sql` — includes `is_potentially_habitable` in insert/update
- `nasa_to_postgres_operator.py` — computes habitability at ingest time (moved from OpenSearch indexer)
- `settings.py` — removed `OPENSEARCH_CONN_ID`, `OPENSEARCH_INDEX`
- Grafana now uses its built-in PostgreSQL datasource — no plugins needed
