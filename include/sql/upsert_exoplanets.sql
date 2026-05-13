INSERT INTO exoplanets (
    pl_name, hostname, sy_snum, sy_pnum, discoverymethod, disc_year,
    pl_orbper, pl_rade, pl_masse, pl_dens, pl_eqt, pl_orbeccen,
    pl_orbsmax, pl_insol, st_teff, st_rad, st_mass, st_met, st_logg,
    sy_dist, sy_vmag, ra, dec, row_checksum, ingested_at, updated_at
)
VALUES %s
ON CONFLICT (pl_name) DO UPDATE SET
    hostname        = EXCLUDED.hostname,
    sy_snum         = EXCLUDED.sy_snum,
    sy_pnum         = EXCLUDED.sy_pnum,
    discoverymethod = EXCLUDED.discoverymethod,
    disc_year       = EXCLUDED.disc_year,
    pl_orbper       = EXCLUDED.pl_orbper,
    pl_rade         = EXCLUDED.pl_rade,
    pl_masse        = EXCLUDED.pl_masse,
    pl_dens         = EXCLUDED.pl_dens,
    pl_eqt          = EXCLUDED.pl_eqt,
    pl_orbeccen     = EXCLUDED.pl_orbeccen,
    pl_orbsmax      = EXCLUDED.pl_orbsmax,
    pl_insol        = EXCLUDED.pl_insol,
    st_teff         = EXCLUDED.st_teff,
    st_rad          = EXCLUDED.st_rad,
    st_mass         = EXCLUDED.st_mass,
    st_met          = EXCLUDED.st_met,
    st_logg         = EXCLUDED.st_logg,
    sy_dist         = EXCLUDED.sy_dist,
    sy_vmag         = EXCLUDED.sy_vmag,
    ra              = EXCLUDED.ra,
    dec             = EXCLUDED.dec,
    row_checksum    = EXCLUDED.row_checksum,
    updated_at      = EXCLUDED.updated_at
WHERE exoplanets.row_checksum != EXCLUDED.row_checksum;
