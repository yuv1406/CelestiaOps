NASA_TAP_BASE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
NASA_TABLE = "pscomppars"

# Columns to fetch from the NASA API
NASA_COLUMNS = [
    "pl_name",
    "hostname",
    "sy_snum",
    "sy_pnum",
    "discoverymethod",
    "disc_year",
    "pl_orbper",
    "pl_rade",
    "pl_masse",
    "pl_dens",
    "pl_eqt",
    "pl_orbeccen",
    "pl_orbsmax",
    "pl_insol",
    "st_teff",
    "st_rad",
    "st_mass",
    "st_met",
    "st_logg",
    "sy_dist",
    "sy_vmag",
    "ra",
    "dec",
]

# Columns used for checksum (content-addressable change detection)
CHECKSUM_COLUMNS = [
    "pl_orbper",
    "pl_rade",
    "pl_masse",
    "pl_eqt",
    "st_teff",
    "st_rad",
    "st_mass",
    "sy_dist",
    "disc_year",
    "discoverymethod",
]

POSTGRES_CONN_ID = "celestiaops_postgres"

# Pagination chunk size for NASA API requests
NASA_FETCH_CHUNK = 5000

# Habitable zone temperature range (K) — conservative estimate
HZ_TEMP_MIN = 180
HZ_TEMP_MAX = 310
