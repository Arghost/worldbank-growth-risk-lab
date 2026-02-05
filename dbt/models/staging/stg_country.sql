{{
    config(
        materialized = 'view'
    )
}}
with base as (
    select country_iso3,
        country_name,
        region,
        income_level,
        lending_type,
        capital_city,
        capital_lat,
        capital_lon,
        DATE(load_ts) AS date_loaded
        from {{ source('wb_raw', 'raw_country_dim') }}
        where load_ts = (select max(load_ts) from {{ source('wb_raw', 'raw_country_dim') }})
),
cleansed as (
    select country_iso3 as country_code,
        country_name,
        region,
        income_level,
        lending_type,
        capital_city,
        capital_lat,
        capital_lon,
        date_loaded
        from base
        where country_iso3 is not null
)

select * from cleansed