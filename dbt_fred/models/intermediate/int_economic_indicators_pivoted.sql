/*
    models/intermediate/int_economic_indicators_pivoted.sql

    Pivots the long-format observations into a wide table:
    one row per month, one column per indicator.

    Why: Raw data has one row per (series_id, date). Downstream models
    need indicators side by side to calculate cross-indicator metrics
    like recession signals.

    Handles mixed frequencies:
    - Monthly series (UNRATE, CPI, FEDFUNDS, UMCSENT): use as-is
    - Quarterly series (GDP): forward-fill to monthly
    - Daily series (T10Y2Y): take month-end value
*/

with

observations as (
    select * from {{ ref('stg_fred_observations') }}
),

-- Generate a complete monthly calendar from our data range
date_spine as (
    select
        dateadd('month', seq4(), '2000-01-01'::date) as observation_month
    from table(generator(rowcount => 500))  -- ~41 years of months
    where dateadd('month', seq4(), '2000-01-01'::date) <= current_date()
),

-- Monthly series: take the value for each month
monthly_values as (
    select
        date_trunc('month', observation_date)::date as observation_month,
        series_id,
        value
    from observations
    where series_id in ('UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'UMCSENT')
),

-- Quarterly GDP: assign to the quarter's first month, then forward-fill
gdp_quarterly as (
    select
        date_trunc('month', observation_date)::date as observation_month,
        value as gdp_value
    from observations
    where series_id = 'GDP'
),

gdp_filled as (
    select
        ds.observation_month,
        last_value(gq.gdp_value ignore nulls) over (
            order by ds.observation_month
            rows between unbounded preceding and current row
        ) as gdp_value
    from date_spine ds
    left join gdp_quarterly gq
        on ds.observation_month = gq.observation_month
),

-- Daily yield spread: take the last value of each month
yield_spread_monthly as (
    select
        date_trunc('month', observation_date)::date as observation_month,
        value as t10y2y_value,
        row_number() over (
            partition by date_trunc('month', observation_date)
            order by observation_date desc
        ) as row_num
    from observations
    where series_id = 'T10Y2Y'
),

yield_spread as (
    select observation_month, t10y2y_value
    from yield_spread_monthly
    where row_num = 1
),

-- Pivot monthly series into columns
pivoted as (
    select
        observation_month,
        max(case when series_id = 'UNRATE'   then value end) as unemployment_rate,
        max(case when series_id = 'CPIAUCSL' then value end) as cpi_index,
        max(case when series_id = 'FEDFUNDS' then value end) as fed_funds_rate,
        max(case when series_id = 'UMCSENT'  then value end) as consumer_sentiment
    from monthly_values
    group by observation_month
),

-- Join everything together
combined as (
    select
        p.observation_month,
        p.unemployment_rate,
        p.cpi_index,
        p.fed_funds_rate,
        p.consumer_sentiment,
        gf.gdp_value                as gdp,
        ys.t10y2y_value             as treasury_spread_10y2y
    from pivoted p
    left join gdp_filled gf
        on p.observation_month = gf.observation_month
    left join yield_spread ys
        on p.observation_month = ys.observation_month
    where p.observation_month is not null
)

select * from combined
order by observation_month
