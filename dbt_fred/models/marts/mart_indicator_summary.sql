/*
    models/marts/mart_indicator_summary.sql

    Summary table showing the latest value, trend, and historical
    context for each indicator. Useful for a "current snapshot"
    panel on the dashboard.

    One row per indicator with:
    - Latest value and date
    - Previous month value
    - MoM and YoY direction
    - Historical min/max/avg
*/

with

latest as (
    select
        observation_month,
        unemployment_rate,
        cpi_index,
        fed_funds_rate,
        consumer_sentiment,
        gdp,
        treasury_spread_10y2y,
        inflation_rate_yoy,
        recession_signal_count,
        recession_risk_level
    from {{ ref('mart_economic_dashboard') }}
    where observation_month = (
        select max(observation_month) 
        from {{ ref('mart_economic_dashboard') }}
        where unemployment_rate is not null
    )
),

previous as (
    select
        observation_month,
        unemployment_rate,
        cpi_index,
        fed_funds_rate,
        consumer_sentiment,
        inflation_rate_yoy
    from {{ ref('mart_economic_dashboard') }}
    where observation_month = (
        select dateadd('month', -1, max(observation_month))
        from {{ ref('mart_economic_dashboard') }}
        where unemployment_rate is not null
    )
),

historical_stats as (
    select
        min(unemployment_rate) as unemployment_min,
        max(unemployment_rate) as unemployment_max,
        round(avg(unemployment_rate), 2) as unemployment_avg,
        min(inflation_rate_yoy) as inflation_min,
        max(inflation_rate_yoy) as inflation_max,
        round(avg(inflation_rate_yoy), 2) as inflation_avg,
        min(fed_funds_rate) as fed_funds_min,
        max(fed_funds_rate) as fed_funds_max,
        round(avg(fed_funds_rate), 2) as fed_funds_avg,
        min(consumer_sentiment) as sentiment_min,
        max(consumer_sentiment) as sentiment_max,
        round(avg(consumer_sentiment), 2) as sentiment_avg
    from {{ ref('mart_economic_dashboard') }}
    where unemployment_rate is not null
),

summary as (
    select
        -- Current period
        l.observation_month                             as latest_month,
        l.recession_risk_level                          as current_risk_level,
        l.recession_signal_count                        as active_signals,

        -- Unemployment
        l.unemployment_rate                             as unemployment_current,
        p.unemployment_rate                             as unemployment_previous,
        l.unemployment_rate - p.unemployment_rate       as unemployment_mom_change,
        h.unemployment_min,
        h.unemployment_max,
        h.unemployment_avg,

        -- Inflation
        l.inflation_rate_yoy                            as inflation_current,
        p.inflation_rate_yoy                            as inflation_previous,
        l.inflation_rate_yoy - p.inflation_rate_yoy     as inflation_mom_change,
        h.inflation_min,
        h.inflation_max,
        h.inflation_avg,

        -- Fed Funds Rate
        l.fed_funds_rate                                as fed_funds_current,
        p.fed_funds_rate                                as fed_funds_previous,
        l.fed_funds_rate - p.fed_funds_rate             as fed_funds_mom_change,
        h.fed_funds_min,
        h.fed_funds_max,
        h.fed_funds_avg,

        -- Consumer Sentiment
        l.consumer_sentiment                            as sentiment_current,
        p.consumer_sentiment                            as sentiment_previous,
        l.consumer_sentiment - p.consumer_sentiment     as sentiment_mom_change,
        h.sentiment_min,
        h.sentiment_max,
        h.sentiment_avg

    from latest l
    cross join previous p
    cross join historical_stats h
)

select * from summary
