"""
Quick smoke-test for LAMOST VizieR integration.

Pulls Kepler host stars from the local exoplanets DB (ideal LAMOST targets:
V≈12-16 mag, northern sky), runs a VizieR ADQL cone search on V/164/stellar5
for each, and prints a compact result table.

Run inside the airflow_worker container:
    docker cp test_lamost_fetch.py airflow_worker:/opt/airflow/
    docker exec airflow_worker python3 /opt/airflow/test_lamost_fetch.py
"""

import time
import warnings

import psycopg2
import requests

warnings.filterwarnings("ignore")

# ── connection (inside Docker network) ────────────────────────────────────────
DB = dict(
    host="celestiaops_timescaledb", port=5432,
    dbname="exoplanets", user="celestia", password="celestia",
)

# ── VizieR LAMOST DR5 stellar parameters (V/164/stellar5, 5.3M rows) ─────────
VIZIER_TAP_URL  = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync"
LAMOST_TABLE    = '"V/164/stellar5"'
CONE_RADIUS_DEG = 5.0 / 3600.0   # 5 arcsec
MAX_OBS         = 3
DELAY_SEC       = 0.3

ADQL = (
    "SELECT TOP {limit} ObsID, RAJ2000, DEJ2000, Teff, e_Teff, logg, e_logg, "
    '"[Fe/H]", "e_[Fe/H]", HRV, e_HRV, snrg, snrr, Class, SubClass, ObsDate '
    "FROM {table} "
    "WHERE CONTAINS("
    "  POINT('ICRS', RAJ2000, DEJ2000),"
    "  CIRCLE('ICRS', {ra}, {dec}, {radius})"
    ") = 1 "
    "ORDER BY snrg DESC"
)


def get_host_stars(n=20):
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        # Kepler stars: typically V=12-16, ideal for LAMOST
        cur.execute(
            """
            SELECT DISTINCT hostname, ra, dec
            FROM exoplanets
            WHERE hostname LIKE 'Kepler-%%'
              AND ra IS NOT NULL AND dec IS NOT NULL
            ORDER BY hostname
            LIMIT %s
            """,
            (n,),
        )
        rows = cur.fetchall()
    conn.close()
    return rows


def query_vizier(ra, dec):
    adql = ADQL.format(
        limit=MAX_OBS, table=LAMOST_TABLE, ra=ra, dec=dec, radius=CONE_RADIUS_DEG
    )
    resp = requests.get(
        VIZIER_TAP_URL,
        params={"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "json", "QUERY": adql},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data") or []
    if not data:
        return []
    cols = [c["name"] for c in payload.get("metadata", [])]
    return [dict(zip(cols, row)) for row in data]


def main():
    stars = get_host_stars(20)
    print(f"Testing {len(stars)} Kepler host stars against LAMOST V/164/stellar5\n")

    header = (
        f"{'Hostname':<22}  {'ObsID':>10}  {'Teff':>7}  {'±':>5}  "
        f"{'logg':>5}  {'[Fe/H]':>6}  {'HRV':>7}  {'SNRg':>6}  "
        f"{'Class':<5}  {'SubClass':<9}  ObsDate"
    )
    print(header)
    print("─" * len(header))

    hits = misses = errors = 0

    for hostname, ra, dec in stars:
        try:
            obs_list = query_vizier(ra, dec)
            if not obs_list:
                misses += 1
                print(f"  {'miss':<22}  {hostname}  (ra={ra:.3f} dec={dec:.3f})")
            else:
                hits += 1
                for obs in obs_list:
                    teff  = obs.get("Teff")
                    e_teff= obs.get("e_Teff")
                    logg  = obs.get("logg")
                    feh   = obs.get("[Fe/H]")
                    hrv   = obs.get("HRV")
                    snrg  = obs.get("snrg")
                    print(
                        f"  {hostname:<22}  "
                        f"{str(obs.get('ObsID') or ''):>10}  "
                        f"{teff if teff else '-':>7}  "
                        f"{e_teff if e_teff else '-':>5}  "
                        f"{logg if logg else '-':>5}  "
                        f"{feh if feh else '-':>6}  "
                        f"{hrv if hrv else '-':>7}  "
                        f"{snrg if snrg else '-':>6}  "
                        f"{str(obs.get('Class') or ''):5}  "
                        f"{str(obs.get('SubClass') or ''):9}  "
                        f"{obs.get('ObsDate') or '-'}"
                    )
            time.sleep(DELAY_SEC)
        except Exception as exc:
            errors += 1
            print(f"  ERROR  {hostname:<20}  {exc}")

    print()
    print(
        f"Result: {len(stars)} queried | "
        f"{hits} with LAMOST data | {misses} no match | {errors} errors"
    )
    if hits:
        print("✓ VizieR ADQL query, column mapping, and cone search all working.")


if __name__ == "__main__":
    main()
