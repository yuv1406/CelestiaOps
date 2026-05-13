-- Main exoplanet table — one row per planet, upserted on each sync
CREATE TABLE IF NOT EXISTS exoplanets (
    pl_name         TEXT PRIMARY KEY,
    hostname        TEXT,
    sy_snum         SMALLINT,
    sy_pnum         SMALLINT,
    discoverymethod TEXT,
    disc_year       SMALLINT,
    pl_orbper       DOUBLE PRECISION,
    pl_rade         DOUBLE PRECISION,
    pl_masse        DOUBLE PRECISION,
    pl_dens         DOUBLE PRECISION,
    pl_eqt          DOUBLE PRECISION,
    pl_orbeccen     DOUBLE PRECISION,
    pl_orbsmax      DOUBLE PRECISION,
    pl_insol        DOUBLE PRECISION,
    st_teff         DOUBLE PRECISION,
    st_rad          DOUBLE PRECISION,
    st_mass         DOUBLE PRECISION,
    st_met          DOUBLE PRECISION,
    st_logg         DOUBLE PRECISION,
    sy_dist         DOUBLE PRECISION,
    sy_vmag         DOUBLE PRECISION,
    ra              DOUBLE PRECISION,
    dec             DOUBLE PRECISION,
    row_checksum             TEXT NOT NULL,
    is_potentially_habitable BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sync state table — tracks the last successful run per DAG
CREATE TABLE IF NOT EXISTS sync_state (
    dag_id          TEXT PRIMARY KEY,
    last_sync_at    TIMESTAMPTZ NOT NULL,
    rows_fetched    INTEGER,
    rows_inserted   INTEGER,
    rows_updated    INTEGER,
    rows_unchanged  INTEGER
);

-- Historical snapshots hypertable (TimescaleDB)
CREATE TABLE IF NOT EXISTS exoplanets_history (
    snapshot_time   TIMESTAMPTZ NOT NULL,
    pl_name         TEXT NOT NULL,
    hostname        TEXT,
    disc_year       SMALLINT,
    pl_orbper       DOUBLE PRECISION,
    pl_rade         DOUBLE PRECISION,
    pl_masse        DOUBLE PRECISION,
    pl_eqt          DOUBLE PRECISION,
    st_teff         DOUBLE PRECISION,
    st_rad          DOUBLE PRECISION,
    st_mass         DOUBLE PRECISION,
    sy_dist         DOUBLE PRECISION,
    row_checksum    TEXT NOT NULL,
    PRIMARY KEY (snapshot_time, pl_name)
);

SELECT create_hypertable(
    'exoplanets_history',
    'snapshot_time',
    if_not_exists => TRUE
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_exoplanets_disc_year        ON exoplanets (disc_year);
CREATE INDEX IF NOT EXISTS idx_exoplanets_discoverymethod  ON exoplanets (discoverymethod);
CREATE INDEX IF NOT EXISTS idx_exoplanets_pl_eqt           ON exoplanets (pl_eqt);
CREATE INDEX IF NOT EXISTS idx_exoplanets_updated_at       ON exoplanets (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_exoplanets_habitable        ON exoplanets (is_potentially_habitable) WHERE is_potentially_habitable = TRUE;
CREATE INDEX IF NOT EXISTS idx_history_pl_name             ON exoplanets_history (pl_name, snapshot_time DESC);

-- GIN index for full-text search across planet name, host star, and discovery method
CREATE INDEX IF NOT EXISTS idx_exoplanets_fts ON exoplanets USING GIN (
    to_tsvector('english',
        coalesce(pl_name, '') || ' ' ||
        coalesce(hostname, '') || ' ' ||
        coalesce(discoverymethod, '')
    )
);
