{{config (materialized = 'view')}}
with base as (
    select country_iso3,
        year,
        indicator_code,
        indicator_name,
        value,
        DATE(load_ts) AS date_loaded
        from {{source('wb_raw', 'raw_wb_indicators_long')}}
        where load_ts = (select max(load_ts) from {{source('wb_raw', 'raw_wb_indicators_long')}})
),
cleansed as (
    select country_iso3 as country_code,
        year,
        indicator_code,
        indicator_name,
        value,
        date_loaded
        from base
        where country_iso3 is not null
)

select * from cleansed