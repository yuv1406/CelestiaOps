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
