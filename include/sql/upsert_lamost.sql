INSERT INTO lamost_observations (
    obsid, hostname, ra, dec,
    teff, e_teff, logg, e_logg, feh, e_feh, hrv, e_hrv,
    snr_g, snr_r, spec_class, spec_subclass, obs_date,
    row_checksum, ingested_at, updated_at
)
VALUES %s
ON CONFLICT (obsid) DO UPDATE SET
    hostname      = EXCLUDED.hostname,
    teff          = EXCLUDED.teff,
    e_teff        = EXCLUDED.e_teff,
    logg          = EXCLUDED.logg,
    e_logg        = EXCLUDED.e_logg,
    feh           = EXCLUDED.feh,
    e_feh         = EXCLUDED.e_feh,
    hrv           = EXCLUDED.hrv,
    e_hrv         = EXCLUDED.e_hrv,
    snr_g         = EXCLUDED.snr_g,
    snr_r         = EXCLUDED.snr_r,
    spec_class    = EXCLUDED.spec_class,
    spec_subclass = EXCLUDED.spec_subclass,
    obs_date      = EXCLUDED.obs_date,
    row_checksum  = EXCLUDED.row_checksum,
    updated_at    = EXCLUDED.updated_at
WHERE lamost_observations.row_checksum != EXCLUDED.row_checksum;
