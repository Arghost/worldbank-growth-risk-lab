"""
World Bank (WDI) -> BigQuery RAW tables.
Creates and loads:
- wb_raw.raw_wb_indicators_long
- wb_raw.raw_country_dim
"""
from __future__ import annotations
import os
import uuid
import datetime as dt
from typing import Dict, List, Tuple
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

WB_BASE = "https://api.worldbank.org/v2"
INDICATORS: Dict[str, str] = {
    # Growth / macro
    "NY.GDP.PCAP.KD.ZG": "GDP per capita growth (annual %)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",

    # Internal fiscal risk
    "GC.DOD.TOTL.GD.ZS": "Central government debt, total (% of GDP)",

    # External risk
    "DT.DOD.DECT.GN.ZS": "External debt stocks (% of GNI)",
    "DT.TDS.DECT.EX.ZS": "Total debt service (% of exports)",
    "FI.RES.TOTL.MO": "Total reserves in months of imports",

    # (Optional support – keep commented until you want them)
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
    "BX.KLT.DINV.WD.GD.ZS": "FDI, net inflows (% of GDP)",
    "IT.NET.USER.ZS": "Individuals using the Internet (% of population)",
    # "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    # "SP.POP.TOTL": "Population, total",
}

def fetch_indicators_long(start_year: int, end_year: int, indicator_codes: List[str]) -> pd.DataFrame:
    """
    Fetch indicator observations in long format.

    Uses multi-indicator call:
    /country/all/indicator/CODE1;CODE2?date=YYYY:YYYY&source=2
    """
    codes = ";".join(indicator_codes)
    url = f"{WB_BASE}/country/all/indicator/{codes}"

    per_page = 20000
    page = 1
    rows = []

    while True:
        data = wb_get_json(
            url,
            params={
                "format": "json",
                "source": 2,  # WDI
                "date": f"{start_year}:{end_year}",
                "per_page": per_page,
                "page": page,
            },
        )
        meta = data[0] if isinstance(data, list) and len(data) > 0 else {}
        items = data[1] if isinstance(data, list) and len(data) > 1 else []
        if not items:
            break

        rows.extend(items)

        if int(meta.get("page", 1)) >= int(meta.get("pages", 1)):
            break
        page += 1

    df = pd.json_normalize(rows)

    # Normalize columns
    rename = {
        "countryiso3code": "country_iso3",
        "country.value": "country_name",
        "date": "year",
        "indicator.id": "indicator_code",
        "indicator.value": "indicator_name",
        "value": "value",
    }
    df = df.rename(columns=rename)

    df = df[["country_iso3", "country_name", "year", "indicator_code", "indicator_name", "value"]].copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Some API rows can have missing country_iso3; drop them
    df = df.dropna(subset=["country_iso3", "year", "indicator_code"]).copy()

    return df.reset_index(drop=True)


def wb_get_json(url: str, params: dict, timeout_s: int = 60) -> list:
    """Basic GET with small retry logic."""
    last_err = None
    for attempt in range(1, 3):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                # backoff
                import time
                time.sleep(min(0.8 * (2 ** (attempt - 1)), 6))
                continue
            r.raise_for_status()
        except Exception as e:
            last_err = e
            import time
            time.sleep(min(0.8 * (2 ** (attempt - 1)), 6))
    raise RuntimeError(f"World Bank API failed. Last error: {last_err}")

def fetch_countries_dim() -> pd.DataFrame:
    """
    Fetch country metadata and exclude aggregates.
    World Bank aggregates usually have region.id == 'NA'.
    """
    url = f"{WB_BASE}/country"
    per_page = 400
    page = 1
    rows = []

    while True:
        data = wb_get_json(url, params={"format": "json", "per_page": per_page, "page": page})
        meta, items = data[0], data[1]
        rows.extend(items)
        if int(meta["page"]) >= int(meta["pages"]):
            break
        page += 1

    df = pd.json_normalize(rows)

    # Exclude aggregates
    df = df[df["region.id"].astype(str) != "NA"].copy()

    keep_cols = {
        "id": "country_iso3",
        "iso2Code": "country_iso2",
        "name": "country_name",
        "region.value": "region",
        "incomeLevel.value": "income_level",
        "lendingType.value": "lending_type",
        "capitalCity": "capital_city",
        "latitude": "capital_lat",
        "longitude": "capital_lon",
    }
    df = df[list(keep_cols.keys())].rename(columns=keep_cols)

    # types
    df["capital_lat"] = pd.to_numeric(df["capital_lat"], errors="coerce")
    df["capital_lon"] = pd.to_numeric(df["capital_lon"], errors="coerce")

    return df.reset_index(drop=True)


def bq_ensure_tables(client: bigquery.Client, project_id: str, dataset_id: str) -> Tuple[str, str]:
    """Create RAW tables if they don't exist."""
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    tbl_ind = bigquery.Table(dataset_ref.table("raw_wb_indicators_long"))
    tbl_cty = bigquery.Table(dataset_ref.table("raw_country_dim"))

    # Indicators table schema
    tbl_ind.schema = [
        bigquery.SchemaField("country_iso3", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("year", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("indicator_code", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("indicator_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("load_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    ]

    # Country dim schema
    tbl_cty.schema = [
        bigquery.SchemaField("country_iso3", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("country_iso2", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("income_level", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("lending_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("capital_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("capital_lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("capital_lon", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("load_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    ]

    # Create if not exists
    for table in (tbl_ind, tbl_cty):
        try:
            client.get_table(table.reference)
        except Exception:
            client.create_table(table)

    return (
        f"{project_id}.{dataset_id}.raw_wb_indicators_long",
        f"{project_id}.{dataset_id}.raw_country_dim",
    )

def bq_append_df(client: bigquery.Client, table_fqn: str, df: pd.DataFrame) -> None:
    """Append a dataframe to a BigQuery table."""
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()  # wait

def main():
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW", "wb_raw")

    if not project_id:
        raise ValueError("Missing env var GCP_PROJECT_ID")
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise ValueError("Missing env var GOOGLE_APPLICATION_CREDENTIALS (service account JSON path)")

    # Choose range (default matches our plan)
    start_year = int(os.getenv("WB_START_YEAR", "2020"))
    end_year = int(os.getenv("WB_END_YEAR", "2025"))

    run_id = str(uuid.uuid4())
    load_ts = dt.datetime.now(dt.timezone.utc)

    client = bigquery.Client(project=project_id)

    ind_table, cty_table = bq_ensure_tables(client, project_id, dataset_raw)

    # 1) Countries dimension
    countries = fetch_countries_dim()
    countries["load_ts"] = load_ts
    countries["run_id"] = run_id

    # 2) Indicator observations
    indicator_codes = list(INDICATORS.keys())
    ind_df = fetch_indicators_long(start_year, end_year, indicator_codes)

    # Filter out aggregates explicitly (keep only ISO3 present in countries)
    valid_iso3 = set(countries["country_iso3"].dropna().astype(str).tolist())
    ind_df = ind_df[ind_df["country_iso3"].astype(str).isin(valid_iso3)].copy()

    ind_df["load_ts"] = load_ts
    ind_df["run_id"] = run_id

    # Append to BigQuery
    print(f"[INFO] Loading countries: {len(countries):,} rows -> {cty_table}")
    bq_append_df(client, cty_table, countries)

    print(f"[INFO] Loading indicators: {len(ind_df):,} rows -> {ind_table}")
    bq_append_df(client, ind_table, ind_df)

    print("[DONE] Phase 2 ingest completed.")
    print(f"run_id={run_id}, load_ts={load_ts.isoformat()}")

if __name__ == "__main__":
    main()