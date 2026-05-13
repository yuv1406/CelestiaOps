# CelestiaOps

A self-hosted, production-grade exoplanet intelligence pipeline. CelestiaOps
pulls confirmed exoplanet data from the NASA Exoplanet Archive on a daily
schedule, detects what actually changed, stores it in TimescaleDB with full
historical snapshots, and surfaces everything through a science-grade Grafana
dashboard.

It is, put plainly, a data engineering project about space — because if you
have to build a pipeline, it might as well be pointed at something interesting.

---

## Why This Exists

The NASA Exoplanet Archive exposes its data through a TAP (Table Access
Protocol) service — a standard used extensively in astronomy that lets you
write ADQL queries (essentially SQL for the sky) over HTTP. The interface is
powerful, but it has some quirks that make naive integration painful:

- **Pagination parameters are silently ignored.** The TAP endpoint accepts
  `TOP` and `OFFSET` as URL parameters, but in practice it returns the full
  dataset on every request regardless. A straightforward paginated fetch loop
  therefore runs forever, re-downloading the same 6,286 rows on every
  iteration. The fix is to embed `TOP N` directly inside the ADQL query string,
  which the server actually respects.

- **The full dataset is not that large.** The Planetary Systems Composite
  Parameters table (`pscomppars`) contains roughly 6,300 confirmed planets
  across 23 columns. It fits comfortably in a single request with a two-second
  response time. There is no reason to paginate it.

- **Not every row changes between runs.** NASA updates the archive as new
  papers are published and measurements are revised. On any given day, the
  vast majority of rows are identical to the day before. Re-writing 6,300 rows
  every day is wasteful; CelestiaOps computes a SHA-256 checksum for each row
  and only upserts records whose checksum has changed.

- **The data has a lot of nulls and that is scientifically correct.** Planet
  mass is only measurable through radial velocity or transit timing variations.
  Radius requires a transit. Temperature requires enough stellar data to model.
  Around 53% of confirmed planets have no measured mass — not a data quality
  problem, just the reality of how exoplanet detection works. The pipeline
  handles this gracefully rather than treating nulls as errors.

---

## Architecture

```
NASA TAP API (ADQL over HTTP)
        |
        v
  Apache Airflow
  (orchestration, scheduling, retry logic)
        |
        v
  NasaToPostgresOperator
  (fetch -> checksum -> upsert)
        |
        v
  TimescaleDB
  (exoplanets + exoplanets_history hypertable)
        |
        v
  Grafana
  (provisioned dashboard, auto-refreshes every hour)
```

Everything runs in Docker. The CelestiaOps services (TimescaleDB, Grafana)
join the existing `airflow-stack_default` network so Airflow workers can reach
them by container name without any extra configuration.

---

## Stack

| Component | Role |
|-----------|------|
| Apache Airflow 3 | DAG orchestration, scheduling, retry logic |
| Python 3.13 | Operator logic, checksum computation, data transformation |
| TimescaleDB (PostgreSQL 16) | Primary store + time-series hypertable for snapshots |
| Grafana 13 | Dashboards, auto-provisioned via config files |
| Docker Compose | Container orchestration for the full stack |
| NASA TAP API | Data source — ADQL queries over HTTP |

---

## DAGs

### `ingest_exoplanets` — Daily at 02:00 UTC

The core ingestion pipeline.

```
ensure_schema  -->  ingest_nasa_data  -->  log_stats
```

**ensure_schema** runs `create_tables.sql` on every execution. It is
idempotent (`CREATE TABLE IF NOT EXISTS`) so it is safe to run repeatedly and
doubles as a schema migration safety net.

**ingest_nasa_data** is the `NasaToPostgresOperator`. It:
1. Fetches the full `pscomppars` table from the NASA TAP endpoint in a single
   request
2. Computes a SHA-256 checksum for each row over the ten scientifically
   significant measurement columns (orbital period, radius, mass, temperature,
   stellar properties, distance, discovery year and method)
3. Flags planets whose equilibrium temperature falls in the conservative
   habitable zone range (180 – 310 K)
4. Upserts all rows into TimescaleDB, skipping any record whose checksum has
   not changed since the last run
5. Records the sync result (rows fetched, inserted, updated) in `sync_state`

**log_stats** pulls the upsert stats from XCom and logs them for observability.

---

### `snapshot_history` — Every Sunday at 06:00 UTC

Captures a point-in-time snapshot of the entire exoplanet catalog.

```
check_data_freshness  -->  create_snapshot  -->  prune_old_snapshots
```

**check_data_freshness** reads the last sync timestamp from `sync_state` and
refuses to snapshot stale data. If the ingest DAG has not run in 36 hours the
task fails explicitly rather than silently snapshotting outdated records.

**create_snapshot** inserts the current state of the `exoplanets` table into
the `exoplanets_history` hypertable, tagged with the current UTC timestamp.
TimescaleDB partitions this table by time automatically, making range queries
across years fast without manual partitioning logic.

**prune_old_snapshots** removes snapshot rows older than five years. TimescaleDB
handles the physical deletion efficiently via chunk management.

---

## Database Schema

### `exoplanets`

One row per confirmed planet. Primary key is `pl_name` (e.g. `Kepler-452 b`).

| Column | Type | Description |
|--------|------|-------------|
| `pl_name` | TEXT PK | Planet name |
| `hostname` | TEXT | Host star name |
| `sy_snum` / `sy_pnum` | SMALLINT | Stars / planets in system |
| `discoverymethod` | TEXT | Transit, Radial Velocity, Imaging, etc. |
| `disc_year` | SMALLINT | Year of confirmed discovery |
| `pl_orbper` | DOUBLE | Orbital period (days) |
| `pl_rade` | DOUBLE | Planet radius (Earth radii, R⊕) |
| `pl_masse` | DOUBLE | Planet mass (Earth masses, M⊕) |
| `pl_eqt` | DOUBLE | Equilibrium temperature (K) |
| `st_teff` | DOUBLE | Host star effective temperature (K) |
| `st_rad` / `st_mass` | DOUBLE | Stellar radius (R☉) and mass (M☉) |
| `sy_dist` | DOUBLE | Distance from Earth (parsecs) |
| `row_checksum` | TEXT | SHA-256 of key measurement columns |
| `is_potentially_habitable` | BOOLEAN | True if pl_eqt is 180 – 310 K |
| `ingested_at` / `updated_at` | TIMESTAMPTZ | Audit timestamps |

### `exoplanets_history`

TimescaleDB hypertable. One row per planet per weekly snapshot. Partitioned by
`snapshot_time`. Retention: 5 years.

### `sync_state`

One row per DAG. Tracks `last_sync_at`, `rows_fetched`, `rows_inserted`,
`rows_updated` for operational visibility.

### Indexes

- B-tree on `disc_year`, `discoverymethod`, `pl_eqt`, `updated_at`
- Partial B-tree on `is_potentially_habitable` (WHERE TRUE only — sparse index)
- GIN full-text search index over `pl_name || hostname || discoverymethod`

---

## Checksum Strategy

The upsert logic uses a content-addressable approach: rather than comparing
every column on every row, a single SHA-256 hash is computed over the ten
columns most likely to be revised as new measurements come in:

```
pl_orbper, pl_rade, pl_masse, pl_eqt,
st_teff, st_rad, st_mass, sy_dist,
disc_year, discoverymethod
```

The PostgreSQL upsert is conditional:

```sql
ON CONFLICT (pl_name) DO UPDATE SET ...
WHERE exoplanets.row_checksum != EXCLUDED.row_checksum;
```

A row that has not changed produces zero I/O beyond the conflict check. On a
typical daily run, the vast majority of the 6,300 rows are untouched.

---

## Grafana Dashboard — Exoplanet Observatory

Auto-provisioned at startup. No manual setup required beyond Grafana being
reachable at `http://localhost:3000` (credentials: `admin / celestia`).

The dashboard is divided into thematic sections:

### Header Stats

Six stat cards give the immediate state of the catalog: total confirmed
planets, habitable zone candidate count, number of G-type (Sun-like) host
stars, multi-planet system count, the distance to the nearest known exoplanet
in light-years (currently 4.24 ly — Proxima Centauri), and the timestamp of
the last successful ingest.

### Classification Donuts

Three donut charts break the catalog down by the axes astronomers care about
most: planet physical class (Rocky, Super-Earth, Sub-Neptune, Neptune-like,
Gas Giant — binned by radius using the Fulton gap boundaries), host star
spectral type (M through A+ by effective temperature), and detection method.
Transit dominates at around 74%, which is not a data bias — it is a reflection
of Kepler and TESS being extraordinarily productive.

### Discovery Timeline

A bar chart of annual confirmed discoveries next to a cumulative timeseries.
The spike around 2014–2016 is Kepler's statistical confirmation wave. The
continued additions post-2020 are predominantly TESS.

### Full Planet Catalog

A searchable, filterable table of up to 2,000 planets with all key columns
including Earth-relative units (R⊕, M⊕), orbital period, equilibrium
temperature (color-coded: blue for frozen, green for habitable, orange for
hot, red for extreme), distance in both parsecs and light-years, and a green
HZ badge for habitable zone candidates. Sortable by any column.

### Habitable Zone Candidates

Filtered to the 160 planets whose equilibrium temperature falls in the
conservative 180 – 310 K range, sorted by absolute temperature difference from
Earth's surface average (288 K). The delta column is color-coded — the closer
to zero, the greener. A useful starting point if you are shopping for a
vacation planet.

### Nearest Exoplanets

The 30 closest confirmed planets by distance, with light-year distances
color-graded from green (nearby) through orange and red (distant). Proxima
Centauri b and d sit at 4.24 ly. The next batch clusters around Barnard's
Star at 5.96 ly.

### Planet Size Class Distribution

Bar chart of planet counts across the five radius bins. The Sub-Neptune bucket
dominates — partly astrophysics (they are genuinely common) and partly
selection bias (they are the sweet spot for transit detection sensitivity).

### Multi-Planet Systems

Table of the most populated star systems, ordered by confirmed planet count.
KOI-351 (Kepler-90) leads with 8 confirmed planets — the only system known to
match the Solar System's count. TRAPPIST-1 appears with 7, all of them
potentially rocky.

### Orbital Period Distribution

Bar chart bucketed from hot Jupiters (under 2 days) out to cold companions
(500 – 1000 days). The overwhelming dominance of the sub-10-day bin is a
direct consequence of the transit method's geometric bias toward short-period
planets — they transit more frequently and are easier to confirm.

### Equilibrium Temperature Distribution

Six bins from frozen (below 180 K) through ultra-hot (above 2000 K). The
habitable zone bin (180 – 310 K) is labeled explicitly. The skew toward high
temperatures reflects both the detection bias toward close-in planets and the
prevalence of hot Jupiters in the confirmed catalog.

### Distance Distribution

Eight parsec-range bins out to 3,000 pc. The 300 – 1,000 pc range is heavily
populated because that is where Kepler stared for four years. Nearby planets
(under 50 pc) are relatively sparse simply because there is less sky volume at
short distances.

### Extreme Planets

Three tables: hottest planets by equilibrium temperature (KELT-9 b tops the
list at 4,050 K — hotter than many stars), largest by radius (some inflated
hot Jupiters exceed 80 R⊕), and most massive by mass with both Earth and
Jupiter units shown. These are not edge cases — they are what the universe
actually builds when given the chance.

### Longest Orbital Periods

The cold outer worlds detected primarily by direct imaging and radial velocity
— methods that work where transit geometry fails. Periods shown in both days
and years for context. Neptune's 164.8-year equivalent orbit is a reasonable
mental anchor.

### M-Dwarf Host Stars

The coolest host stars in the catalog (below 3,700 K), ordered by temperature.
Red dwarfs are the most common stellar type in the galaxy and are particularly
interesting for habitability discussions because their habitable zones are
extremely close-in, making transit detection of potentially rocky planets more
likely.

---

## Project Structure

```
CelestiaOps/
    dags/
        ingest_exoplanets.py       # Daily NASA TAP ingest
        snapshot_history.py        # Weekly point-in-time snapshots
        test_ingest_exoplanets.py  # Test variant — writes CSV to results/
        test_snapshot_history.py   # Test variant — reads CSV, writes snapshot CSV
    plugins/
        operators/
            nasa_to_postgres_operator.py  # Production operator
            nasa_to_csv_operator.py       # Test operator (no DB required)
    include/
        config/
            settings.py    # Column lists, API URL, connection IDs, HZ bounds
        sql/
            create_tables.sql      # Idempotent schema setup
            upsert_exoplanets.sql  # Conditional upsert (checksum-gated)
    grafana/
        provisioning/
            datasources/timescaledb.yaml  # Auto-provisions TimescaleDB connection
            dashboards/celestiaops.yaml   # Dashboard provider config
        dashboards/
            exoplanet_overview.json       # Full observatory dashboard
    docker-compose.yml   # TimescaleDB + Grafana, joins airflow-stack network
    requirements.txt
    mission_log.md       # Architecture decisions and incident record
    README.md
```

---

## Running Locally

### Prerequisites

- Docker and Docker Compose
- A running `airflow-stack` with the `airflow-stack_default` network available
- The `airflow-stack` services have `apache-airflow-providers-postgres` installed

### Start the Data Services

```bash
cd CelestiaOps
docker compose up -d
```

This starts TimescaleDB on port `5433` and Grafana on port `3000`.

### Wire up Airflow

Copy the DAGs, operator, and include config into the Airflow stack:

```bash
# From the airflow-stack root
cp CelestiaOps/dags/ingest_exoplanets.py dags/
cp CelestiaOps/dags/snapshot_history.py dags/
cp CelestiaOps/plugins/operators/nasa_to_postgres_operator.py plugins/operators/
cp -r CelestiaOps/include include/
```

Ensure `include/` is mounted in `docker-compose.yml`:

```yaml
volumes:
  - ./include:/opt/airflow/include
```

### Create the Airflow Connection

In the Airflow UI under Admin > Connections, add:

| Field | Value |
|-------|-------|
| Connection Id | `celestiaops_postgres` |
| Connection Type | Postgres |
| Host | `celestiaops_timescaledb` |
| Database | `exoplanets` |
| Login | `celestia` |
| Password | `celestia` |
| Port | `5432` |

### First Run

Trigger `ingest_exoplanets` manually. It will create the schema on first run,
then fetch and load the full catalog. Expect around 15 – 20 seconds for the
NASA API request and another few seconds for the upsert.

Open `http://localhost:3000` (admin / celestia) and the dashboard will be
waiting.

---

## Testing Without a Database

The `test/file-output` branch contains two test DAGs that run the full fetch
and transform pipeline without needing TimescaleDB:

- `test_ingest_exoplanets` — fetches 500 rows from the API (using `SELECT TOP
  500` in the ADQL query) and writes them to `results/exoplanets_<run_id>.csv`
- `test_snapshot_history` — reads the most recent ingest CSV, checks its
  freshness, and writes a snapshot CSV to `results/snapshot_<timestamp>.csv`

Both are `schedule=None` (manual trigger only) and require no connections
beyond the NASA API being reachable.

---

## Branches

| Branch | Purpose |
|--------|---------|
| `main` | Production-ready code |
| `fix/production-dag-bugs` | Merged — import path fixes, pagination fix, timezone coercion |
| `test/file-output` | Test DAGs and CSV operator for validation without a database |

---

## Key Design Decisions

**Why TimescaleDB instead of vanilla PostgreSQL?** The `exoplanets_history`
table is a time-series: one full snapshot per week, indefinitely. TimescaleDB's
automatic chunk partitioning by time makes range queries across years fast
without any manual partitioning logic, and the chunk-based deletion in
`prune_old_snapshots` is significantly more efficient than a full DELETE scan.

**Why no OpenSearch?** Initially included for full-text search. Dropped after
profiling: OpenSearch requires 700 MB – 1 GB of JVM heap for a dataset that
fits in 20 MB of PostgreSQL storage. A GIN index over `pl_name || hostname ||
discoverymethod` covers all realistic search patterns with zero extra
infrastructure.

**Why checksum-based diffing instead of a timestamp?** The NASA archive does
not expose a reliable `last_modified` field per row. The only way to know if a
measurement changed is to compare the measurement itself. SHA-256 over the ten
key numeric columns is cheap to compute in Python, deterministic, and produces
a single value to compare per row.

**Why Grafana provisioning files instead of UI configuration?** Provisioned
datasources and dashboards are version-controlled, reproducible, and
automatically applied on container start. There is no manual click-through to
reproduce the setup on a fresh deployment.
