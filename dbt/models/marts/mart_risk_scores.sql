{{
  config(materialized='table')
}}

with base as (
  select
    country_code,
    year,
    inflation,
    gov_debt_gdp,
    ext_debt_gni,
    debt_service_exports,
    reserves_months_imports,
    date_loaded
  from {{ ref('int_country_year_wide') }}
),

-- Percentile-based scores per year (0-100).
-- Higher score = higher risk. Reserves is inverted (higher reserves => lower risk).
scored as (
  select
    country_code,
    year,
    date_loaded,

    -- NOTE: percent_rank() returns 0..1; multiply by 100 for 0..100 scale.
    case
      when inflation is null then null
      else 100 * percent_rank() over (partition by year order by inflation)
    end as inflation_score,

    case
      when gov_debt_gdp is null then null
      else 100 * percent_rank() over (partition by year order by gov_debt_gdp)
    end as gov_debt_score,

    case
      when ext_debt_gni is null then null
      else 100 * percent_rank() over (partition by year order by ext_debt_gni)
    end as ext_debt_score,

    case
      when debt_service_exports is null then null
      else 100 * percent_rank() over (partition by year order by debt_service_exports)
    end as debt_service_score,

    case
      when reserves_months_imports is null then null
      else 100 * (1 - percent_rank() over (partition by year order by reserves_months_imports))
    end as reserves_score

  from base
),

agg as (
  select
    country_code,
    year,
    date_loaded,

    -- component counts (coverage)
    (case when inflation_score is null then 0 else 1 end
     + case when gov_debt_score is null then 0 else 1 end) as internal_components,

    (case when ext_debt_score is null then 0 else 1 end
     + case when debt_service_score is null then 0 else 1 end
     + case when reserves_score is null then 0 else 1 end) as external_components,

    -- internal average using available components
    safe_divide(
      coalesce(inflation_score, 0) + coalesce(gov_debt_score, 0),
      nullif(
        (case when inflation_score is null then 0 else 1 end
         + case when gov_debt_score is null then 0 else 1 end),
        0
      )
    ) as risk_internal_score,

    -- external average using available components
    safe_divide(
      coalesce(ext_debt_score, 0) + coalesce(debt_service_score, 0) + coalesce(reserves_score, 0),
      nullif(
        (case when ext_debt_score is null then 0 else 1 end
         + case when debt_service_score is null then 0 else 1 end
         + case when reserves_score is null then 0 else 1 end),
        0
      )
    ) as risk_external_score

  from scored
),

final as (
  select
    *,
    internal_components + external_components as overall_components,

    -- overall average using available internal/external blocks
    safe_divide(
      coalesce(risk_internal_score, 0) + coalesce(risk_external_score, 0),
      nullif(
        (case when risk_internal_score is null then 0 else 1 end
         + case when risk_external_score is null then 0 else 1 end),
        0
      )
    ) as risk_overall_score
  from agg
)

select
  *,
  case
    -- optional guardrail: only label if we have enough signal
    when overall_components < 3 then null
    when risk_overall_score >= 70 then 1
    else 0
  end as high_risk_flag,
  case
    when overall_components < 3 then 'Insufficient data'
    when risk_overall_score >= 70 then 'High'
    when risk_overall_score >= 40 then 'Medium'
    else 'Low'
  end as risk_band
from final