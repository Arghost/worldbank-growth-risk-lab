{{
  config(
    materialized = 'table',
    )
}}
with base as (
    select
        country_code,
        year,
        -- Core
        max(case when indicator_code = 'NY.GDP.PCAP.KD.ZG' then value end) as gdp_pc_growth,
        max(case when indicator_code = 'NY.GDP.MKTP.KD.ZG' then value end) as gdp_growth,
        max(case when indicator_code = 'FP.CPI.TOTL.ZG' then value end) as inflation,
        max(case when indicator_code = 'GC.DOD.TOTL.GD.ZS' then value end) as gov_debt_gdp,
        max(case when indicator_code = 'DT.DOD.DECT.GN.ZS' then value end) as ext_debt_gni,
        max(case when indicator_code = 'DT.TDS.DECT.EX.ZS' then value end) as debt_service_exports,
        max(case when indicator_code = 'FI.RES.TOTL.MO' then value end) as reserves_months_imports,
        -- Support (high value)
        max(case when indicator_code = 'SL.UEM.TOTL.ZS' then value end) as unemployment,
        max(case when indicator_code = 'NE.TRD.GNFS.ZS' then value end) as trade_gdp,
        max(case when indicator_code = 'BX.KLT.DINV.WD.GD.ZS' then value end) as fdi_inflows_gdp,
        max(case when indicator_code = 'IT.NET.USER.ZS' then value end) as internet_users,
        -- Metadata
        max(date_loaded) as date_loaded
        from {{ ref('stg_indicators') }}
        group by country_code, year
)

select * from base