/*
    models/intermediate/int_indicators_with_changes.sql

    Adds derived time-series metrics on top of the pivoted indicators:
    - Month-over-month changes (absolute and percentage)
    - Year-over-year changes
    - Rolling averages (3-month and 12-month)
    - CPI-derived inflation rate

    These are the metrics analysts and economists actually look at —
    raw levels alone don't tell you much without trend context.
*/

with

pivoted as (
    select * from {{ ref('int_economic_indicators_pivoted') }}
),

with_changes as (
    select
        observation_month,

        -- ============================================
        -- Raw indicator values
        -- ============================================
        unemployment_rate,
        cpi_index,
        fed_funds_rate,
        consumer_sentiment,
        gdp,
        treasury_spread_10y2y,

        -- ============================================
        -- Unemployment: MoM and YoY changes
        -- ============================================
        unemployment_rate - lag(unemployment_rate, 1) over (order by observation_month)
            as unemployment_rate_mom_change,

        unemployment_rate - lag(unemployment_rate, 12) over (order by observation_month)
            as unemployment_rate_yoy_change,

        avg(unemployment_rate) over (
            order by observation_month
            rows between 2 preceding and current row
        ) as unemployment_rate_3m_avg,

        avg(unemployment_rate) over (
            order by observation_month
            rows between 11 preceding and current row
        ) as unemployment_rate_12m_avg,

        -- 12-month low (used for Sahm Rule)
        min(unemployment_rate) over (
            order by observation_month
            rows between 11 preceding and current row
        ) as unemployment_rate_12m_low,

        -- ============================================
        -- CPI: inflation metrics
        -- ============================================
        -- YoY inflation rate (the number you see in the news)
        round(
            (cpi_index - lag(cpi_index, 12) over (order by observation_month))
            / nullif(lag(cpi_index, 12) over (order by observation_month), 0) * 100,
            2
        ) as inflation_rate_yoy,

        -- MoM inflation (annualized)
        round(
            (cpi_index - lag(cpi_index, 1) over (order by observation_month))
            / nullif(lag(cpi_index, 1) over (order by observation_month), 0) * 1200,
            2
        ) as inflation_rate_mom_annualized,

        -- ============================================
        -- Fed Funds Rate: changes
        -- ============================================
        fed_funds_rate - lag(fed_funds_rate, 1) over (order by observation_month)
            as fed_funds_rate_mom_change,

        fed_funds_rate - lag(fed_funds_rate, 12) over (order by observation_month)
            as fed_funds_rate_yoy_change,

        -- ============================================
        -- Consumer Sentiment: changes and rolling avg
        -- ============================================
        consumer_sentiment - lag(consumer_sentiment, 1) over (order by observation_month)
            as sentiment_mom_change,

        avg(consumer_sentiment) over (
            order by observation_month
            rows between 2 preceding and current row
        ) as sentiment_3m_avg,

        -- ============================================
        -- GDP: quarter-over-quarter growth rate
        -- ============================================
        round(
            (gdp - lag(gdp, 3) over (order by observation_month))
            / nullif(lag(gdp, 3) over (order by observation_month), 0) * 100,
            2
        ) as gdp_qoq_growth_rate,

        -- ============================================
        -- Treasury Spread: rolling average
        -- ============================================
        avg(treasury_spread_10y2y) over (
            order by observation_month
            rows between 2 preceding and current row
        ) as treasury_spread_3m_avg

    from pivoted
)

select * from with_changes
order by observation_month
