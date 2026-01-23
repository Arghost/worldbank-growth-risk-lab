# Project Definition

## Project Name

Global Growth & Risk Forecast Lab (World Bank + BigQuery + dbt + scikit-learn + Tableau)

## Purpose

Build a one-shot, analytics + ML demo that:

- Ingests World Bank (WDI) indicators for 2020+ into BigQuery
- Builds a Bronze/Silver/Gold model in BigQuery using dbt (with tests + documentation)
- Produces (1) growth forecasts and (2) sovereign risk proxies (internal and external)
- Trains and selects the best ML model using Python (scikit-learn)
- Serves final outputs to Tableau via BigQuery tables

## Scope

### In scope

- Time range: 2020 to the latest year available in WDI (typically 2024/2025 partial)
- Global coverage: all countries available in WDI (excluding aggregates)
- Risk proxies:
  - Internal fiscal risk proxy (government debt stress)
  - External debt risk proxy (external vulnerability stress)
- ML:
  - Model tournament (multiple algorithms)
  - Best model selection based on agreed metrics and temporal validation
- Tableau dashboards connected to BigQuery Gold/Scoring tables

### Out of scope

- Daily/weekly automation and scheduling (Will be incorporated next)
- Using BigQuery ML (BQML) for training (I am doing my own training in python to evaluate different algorithms)
- NLP features or text-based signals

## Data Sources

Primary source: World Bank World Development Indicators (WDI), accessed via World Bank API.

## Core Indicators (Data Contract)

Each indicator is used as a feature, target, or risk component.

### Growth / Macro Pressure

- NY.GDP.PCAP.KD.ZG — GDP per capita growth (annual %): Primary growth target and key macro signal for forecasting.
- NY.GDP.MKTP.KD.ZG — GDP growth (annual %): Macro context feature for capacity-to-grow and debt sustainability context.
- FP.CPI.TOTL.ZG — Inflation, consumer prices (annual %): Macro stress proxy; higher inflation generally increases fiscal/financial fragility.

### Internal Fiscal Risk (core)

- GC.DOD.TOTL.GD.ZS — Central government debt, total (% of GDP): Core internal debt burden measure; higher values increase internal fiscal risk.

### External Debt Risk (core)

- DT.DOD.DECT.GN.ZS — External debt stocks (% of GNI): Structural external debt exposure; higher values increase external vulnerability.
- DT.TDS.DECT.EX.ZS — Total debt service (% of exports): External payment pressure; higher values increase external risk.
- FI.RES.TOTL.MO — Total reserves in months of imports: Liquidity buffer; higher values reduce external risk.

## Targets

### Primary target (regression)

- gdp_pc_growth_next_year: GDP per capita growth at year t+1 (derived from NY.GDP.PCAP.KD.ZG).

### Risk flags (Definition)

High risk is defined using a fixed threshold:

- high_internal_risk_flag = 1 if internal_risk_score >= 70 else 0
- high_external_risk_flag = 1 if external_risk_score >= 70 else 0
- high_overall_risk_flag = 1 if overall_risk_score >= 70 else 0

## Risk Scoring Methodology (This is just me, not following a particular methodology)

This project produces transparent, auditable risk proxies based on public indicators.
Scores are on a 0–100 scale.

### Normalization

Each component indicator is transformed into a 0–100 subscore within each year.
Indicators:

- Higher debt / higher debt service / higher inflation => higher risk
- Higher reserves / higher growth => lower risk (inverted where appropriate)

### InternalRiskScore (0–100)

- 70%: subscore(GC.DOD.TOTL.GD.ZS)
- 30%: macro stress subscore (inflation high + growth low)

### ExternalRiskScore (0–100)

- 40%: subscore(DT.DOD.DECT.GN.ZS)
- 40%: subscore(DT.TDS.DECT.EX.ZS)
- 20%: subscore(FI.RES.TOTL.MO) [inverted]

### OverallRiskScore (0–100)

- overall_risk_score = 0.5 _ internal_risk_score + 0.5 _ external_risk_score

## Data Coverage Rules

Because some indicators may be missing for certain countries/years:

- Internal risk requires debt + at least one macro input (growth or inflation) to compute.
- External risk requires at least 2 of 3 external components to compute.
  If insufficient coverage:
- risk score is NULL
- coverage_flag = 'LOW_COVERAGE'
  Tableau will surface coverage_flag in tooltips.

## Data Architecture (BigQuery + dbt)

Datasets:

- wb_raw: raw ingested tables
- wb_analytics: dbt models (staging, intermediate, marts)

Layers (medallion approach):

- Bronze (wb_raw): raw indicator records (long format) + country dimension
- Silver (dbt stg*/int*): typing, filtering, reshaping, feature generation
- Gold (dbt mart\_): ML-ready features, risk scores, targets

Python writes back:

- mart_ml_leaderboard
- mart_ml_scoring
- mart_feature_importance

## dbt Testing Strategy

Minimum tests:

- stg_wb_indicators_long:
  - not_null: country_iso3, year, indicator_code
  - relationships: country_iso3 exists in stg_country_dim
  - accepted range: year >= 2020
- mart_country_year_features:
  - unique: (country_iso3, year)
  - not_null: keys + region + income_level
- mart_risk_scores:
  - score ranges: 0..100
  - flags in {0,1}
  - grade in accepted set
- mart_targets:
  - label in {0,1}
  - t+1 target existence checks where applicable

## Success Criteria

- End-to-end run produces:
  - Gold tables (features, risk scores, targets) in BigQuery
  - ML leaderboard and scoring tables in BigQuery
  - Tableau dashboard with maps + leaderboard + country profile
- Repo contains:
  - clear documentation
  - dbt tests passing
  - reproducible steps to run the lab

## Definition of Done

- Scores created
- Tableau dashboards available to the public
- DBT, GCP processes executed.
- ML algorithms created, compared and selected.
- EDA available for public review/consultation and validation.
