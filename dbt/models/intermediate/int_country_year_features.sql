{{
  config(
    materialized = 'table',
    )
}}
with base as (
    select
    country_code,
    year,
    -- base features (all 11)
    gdp_pc_growth,
    gdp_growth,
    inflation,
    gov_debt_gdp,
    ext_debt_gni,
    debt_service_exports,
    reserves_months_imports,
    unemployment,
    trade_gdp,
    fdi_inflows_gdp,
    internet_users,
    date_loaded
  from {{ ref('int_country_year_wide') }} 
),
with_lags as (
    select
        *,
        -- lags (core 7) computed once
        lag(gdp_pc_growth) over (partition by country_code order by year) as lag_gdp_pc_growth,
        lag(gdp_growth) over (partition by country_code order by year) as lag_gdp_growth,
        lag(inflation) over (partition by country_code order by year) as lag_inflation,
        lag(gov_debt_gdp) over (partition by country_code order by year) as lag_gov_debt_gdp,
        lag(ext_debt_gni) over (partition by country_code order by year) as lag_ext_debt_gni,
        lag(debt_service_exports) over (partition by country_code order by year) as lag_debt_service_exports,
        lag(reserves_months_imports) over (partition by country_code order by year) as lag_reserves_months_imports
    from base
)

select
  country_code,
  year,
  -- base features
  gdp_pc_growth,
  gdp_growth,
  inflation,
  gov_debt_gdp,
  ext_debt_gni,
  debt_service_exports,
  reserves_months_imports,
  unemployment,
  trade_gdp,
  fdi_inflows_gdp,
  internet_users,
  -- lags
  lag_gdp_pc_growth,
  lag_gdp_growth,
  lag_inflation,
  lag_gov_debt_gdp,
  lag_ext_debt_gni,
  lag_debt_service_exports,
  lag_reserves_months_imports,
  -- deltas YoY using lag cols
  gdp_pc_growth - lag_gdp_pc_growth as d_gdp_pc_growth,
  gdp_growth - lag_gdp_growth as d_gdp_growth,
  inflation - lag_inflation as d_inflation,
  gov_debt_gdp - lag_gov_debt_gdp as d_gov_debt_gdp,
  ext_debt_gni - lag_ext_debt_gni as d_ext_debt_gni,
  debt_service_exports - lag_debt_service_exports as d_debt_service_exports,
  reserves_months_imports - lag_reserves_months_imports as d_reserves_months_imports,
  -- metadata
  date_loaded
from with_lags