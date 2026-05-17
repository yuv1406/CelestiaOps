-- LAMOST host-star observations table
-- One row per LAMOST spectrum observation of a planet-hosting star.
-- Source: V/164/stellar5 on VizieR (LAMOST DR5 stellar parameters, 5.3M rows)
-- obsid is the LAMOST observation ID (globally unique per spectrum).
CREATE TABLE IF NOT EXISTS lamost_observations (
    obsid           BIGINT          PRIMARY KEY,
    hostname        TEXT            NOT NULL,  -- matched to exoplanets.hostname
    ra              DOUBLE PRECISION,
    dec             DOUBLE PRECISION,
    teff            DOUBLE PRECISION,          -- effective temperature (K)
    e_teff          DOUBLE PRECISION,          -- uncertainty in Teff
    logg            DOUBLE PRECISION,          -- log surface gravity (cgs)
    e_logg          DOUBLE PRECISION,          -- uncertainty in logg
    feh             DOUBLE PRECISION,          -- metallicity [Fe/H] (dex)
    e_feh           DOUBLE PRECISION,          -- uncertainty in [Fe/H]
    hrv             DOUBLE PRECISION,          -- heliocentric radial velocity (km/s)
    e_hrv           DOUBLE PRECISION,          -- uncertainty in HRV
    snr_g           DOUBLE PRECISION,          -- SNR in g-band
    snr_r           DOUBLE PRECISION,          -- SNR in r-band
    spec_class      TEXT,                      -- STAR / GALAXY / QSO / UNKNOWN
    spec_subclass   TEXT,                      -- e.g. G2, K5, F9
    obs_date        TEXT,                      -- observation date string from LAMOST
    row_checksum    TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Sync state — mirrors the pattern in the exoplanets database
CREATE TABLE IF NOT EXISTS lamost_sync_state (
    dag_id          TEXT        PRIMARY KEY,
    last_sync_at    TIMESTAMPTZ NOT NULL,
    stars_queried   INTEGER,
    obs_inserted    INTEGER,
    obs_updated     INTEGER
);

-- Historical snapshots — TimescaleDB hypertable partitioned by snapshot_time
CREATE TABLE IF NOT EXISTS lamost_obs_history (
    snapshot_time   TIMESTAMPTZ     NOT NULL,
    obsid           BIGINT          NOT NULL,
    hostname        TEXT            NOT NULL,
    teff            DOUBLE PRECISION,
    logg            DOUBLE PRECISION,
    feh             DOUBLE PRECISION,
    row_checksum    TEXT            NOT NULL,
    PRIMARY KEY (snapshot_time, obsid)
);

SELECT create_hypertable(
    'lamost_obs_history',
    'snapshot_time',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_lamost_hostname   ON lamost_observations (hostname);
CREATE INDEX IF NOT EXISTS idx_lamost_teff       ON lamost_observations (teff);
CREATE INDEX IF NOT EXISTS idx_lamost_spec_class ON lamost_observations (spec_class);
CREATE INDEX IF NOT EXISTS idx_lamost_updated_at ON lamost_observations (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_lamost_hist_host  ON lamost_obs_history  (hostname, snapshot_time DESC);
