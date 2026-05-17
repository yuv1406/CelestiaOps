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

## 2026-05-13 — Search Engine: OpenSearch Replaced with PostgreSQL FTS

**Decision:** Dropped OpenSearch entirely. Replaced with a GIN full-text search index on TimescaleDB. Deleted `dags/index_opensearch.py`.

**Why:** OpenSearch requires ~700 MB–1 GB RAM for its JVM alone. The exoplanet dataset is structured — queries are range filters (mass, temperature, year) plus keyword lookup on planet/star names. PostgreSQL FTS with a GIN index covers all of this natively with zero extra containers or RAM. The `index_opensearch` DAG was a sync layer that now has no purpose.

**What changed:**
- `create_tables.sql` — added `is_potentially_habitable BOOLEAN` column, partial index on it, GIN FTS index on `pl_name + hostname + discoverymethod`
- `upsert_exoplanets.sql` — includes `is_potentially_habitable` in insert/update
- `nasa_to_postgres_operator.py` — computes habitability at ingest time (moved from OpenSearch indexer)
- `settings.py` — removed `OPENSEARCH_CONN_ID`, `OPENSEARCH_INDEX`
- Grafana now uses its built-in PostgreSQL datasource — no plugins needed

---

## 2026-05-17 — LAMOST DR5 Pipeline Added

**Decision:** Added a full LAMOST spectral ingestion pipeline alongside the existing NASA exoplanet pipeline.

**Why:** LAMOST DR5 (V/164/stellar5, 5.3M rows) contains Teff, logg, [Fe/H], and HRV for FGK stars — enriching exoplanet host stars with spectroscopic parameters that NASA TAP doesn't provide. Coverage is northern sky (dec > −10°), V ≈ 10–17.8 mag; matched ~1,003 of 4,708 host stars on first run.

**What changed:**
- `plugins/operators/lamost_to_postgres_operator.py` — VizieR TAP cone search (2-arcsec radius) per host star, SHA-256 checksum upsert into `lamost` TimescaleDB database
- `include/config/lamost_settings.py` — VizieR endpoint, LAMOST table, columns, cone radius, rate-limit delay
- `include/sql/create_lamost_tables.sql` — `lamost_observations` table, `lamost_sync_state`, `lamost_obs_history` hypertable (partitioned by `snapshot_time`)
- `include/sql/upsert_lamost.sql` — upsert on `obsid`, skip-on-unchanged via checksum
- `dags/ingest_lamost_stars.py` — `ensure_lamost_db → ingest_lamost_data → log_stats`

**Schedule decision:** Weekly (Monday 05:00 UTC), not daily. LAMOST DR5 is a static catalog release — daily runs would be 4,700+ no-op VizieR queries after the first sync. Weekly cadence still catches newly discovered host stars added by `ingest_exoplanets`.

---

## 2026-05-17 — LAMOST CSV Test Operator Added

**Decision:** Added `LamostToCsvOperator` and `test_ingest_lamost_stars` DAG before committing to live database writes.

**Why:** Same validation pattern used for NASA (`NasaToCsvOperator`). Fetches from VizieR and writes to `results/lamost_<run_id>.csv` with no DB dependency. CSV was validated before running the real ingest — 1,681 rows, 1,003 unique host stars, 0 errors, all `spec_class = STAR`.

---

## 2026-05-17 — snapshot_history Merged to Cover Both Catalogs

**Decision:** Extended `snapshot_history` DAG to snapshot both `exoplanets_history` and `lamost_obs_history` in two parallel branches. Removed the separate `snapshot_lamost_history` DAG.

**Why:** One DAG is simpler to monitor and maintain. The branches are independent — a LAMOST freshness failure won't block the exoplanets snapshot.

**Schedule moved:** Sunday 06:00 → Monday 08:00 UTC. LAMOST ingest runs Monday 05:00; the 3-hour buffer ensures it finishes before the snapshot gate checks `lamost_sync_state`. Exoplanets freshness gate (36h) is unaffected since `ingest_exoplanets` runs daily at 02:00.

**Weekly pipeline order (Mondays):**
- 02:00 — `ingest_exoplanets` (daily, unchanged)
- 05:00 — `ingest_lamost_stars` (weekly)
- 08:00 — `snapshot_history` (both branches)

---

## 2026-05-17 — LAMOST Grafana Dashboard Added

**Decision:** Created `lamost_spectroscopy.json` — a 24-panel Grafana dashboard
for LAMOST spectroscopic data, with an exoplanet context section sourced from
the NASA catalog.

**Why:** The LAMOST pipeline had been ingesting data since earlier today but had
no visualisation layer. The exoplanet overview dashboard covers the NASA catalog;
a dedicated LAMOST dashboard exposes stellar parameters (Teff, log g, [Fe/H],
HRV), spectral classification, data quality (SNR), and the subset of exoplanet
hosts observable by LAMOST.

**What changed:**
- `grafana/provisioning/datasources/timescaledb.yaml` — added second datasource
  `CelestiaOps LAMOST` (uid: `celestiaops-lamost`) pointing to the `lamost` DB.
  Same host/credentials as the existing `CelestiaOps` datasource, different
  `jsonData.database`.
- `grafana/dashboards/lamost_spectroscopy.json` — new dashboard (uid:
  `celestiaops-lamost`), 24 panels across five sections:
  1. LAMOST stats (6 stat cards: observations, host stars, mean Teff, mean [Fe/H], avg SNR, SNR > 20 count)
  2. Spectral classification (class donut, subclass bar, Teff-by-type bar)
  3. Parameter distributions (metallicity, log g, radial velocity — all bar charts)
  4. Best observations table (top 50 by SNR, colour-coded SNR column, filterable)
  5. Exoplanet context (6 stats + discovery methods donut + planet size donut + LAMOST-range hosts table from NASA catalog)

**Incident — datasource not applying on file reload:**
After adding the second datasource entry to `timescaledb.yaml`, Grafana
provisioned the `celestiaops-lamost` UID (queries reached the server) but all
panels returned `relation "lamost_observations" does not exist` — meaning
Grafana was connecting to the `exoplanets` database rather than `lamost`. Root
cause: Grafana's file-based datasource provisioning hot-reloads *updates* to
existing entries but does not register *new* datasource entries without a
restart. Fix: `docker restart celestiaops_grafana`. Post-restart logs confirmed
`inserting datasource from configuration name="CelestiaOps LAMOST"`.

**Dashboard design decision — no cross-database joins:**
The LAMOST and exoplanet panels use independent datasources rather than
cross-database SQL. PostgreSQL requires `dblink` or FDW for cross-database
queries; both add complexity for no meaningful gain here. The `hostname` column
links the datasets conceptually — the exoplanet context panels in the dashboard
provide that link visually without a live JOIN.
