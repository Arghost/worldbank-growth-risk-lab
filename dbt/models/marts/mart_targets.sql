{{
  config(
    materialized = 'table',
    )
}}
with base as (
    select *
    from {{ ref('int_country_year_features') }}
),
labeled as (
    select *,
    lead(gdp_pc_growth) over (partition by country_code order by year) as target_gdp_pc_growth_next_year
    from base
)

select * from labeled where target_gdp_pc_growth_next_year is not null