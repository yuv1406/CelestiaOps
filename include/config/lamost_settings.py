# VizieR TAP endpoint (CDS Strasbourg) — hosts the LAMOST public catalogs
VIZIER_TAP_URL = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync"

# LAMOST DR5 stellar parameters catalog on VizieR (5.3M rows, FGK stars)
# Contains Teff, logg, [Fe/H], HRV derived from LAMOST LRS spectra
LAMOST_TABLE = '"V/164/stellar5"'

# Columns to fetch via ADQL.
# [Fe/H] and e_[Fe/H] contain special chars and must be double-quoted in ADQL.
LAMOST_COLUMNS = [
    "ObsID",
    "RAJ2000",
    "DEJ2000",
    "Teff",
    "e_Teff",
    "logg",
    "e_logg",
    '"[Fe/H]"',
    '"e_[Fe/H]"',
    "HRV",
    "e_HRV",
    "snrg",
    "snrr",
    "Class",
    "SubClass",
    "ObsDate",
]

# Columns used for checksum — drives skip-on-unchanged logic.
# These are raw VizieR column names (no ADQL quoting needed for dict access).
LAMOST_CHECKSUM_COLUMNS = ["Teff", "logg", "[Fe/H]", "HRV"]

# Cone search radius in degrees (5 arcseconds — wider than NASA to allow
# slight proper-motion offset between LAMOST obs and catalog positions)
LAMOST_CONE_RADIUS_DEG = 5.0 / 3600.0

# Maximum observations per host star to store (ranked by g-band SNR descending)
LAMOST_MAX_OBS_PER_STAR = 5

# Polite delay between VizieR requests to avoid rate limiting
LAMOST_QUERY_DELAY_SEC = 0.3

LAMOST_CONN_ID = "celestiaops_lamost_postgres"
