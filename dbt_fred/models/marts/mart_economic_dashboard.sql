/*
    models/marts/mart_economic_dashboard.sql

    Main dashboard table — one row per month with all indicators,
    derived metrics, and recession signals joined together.
    
    This is the single table the Streamlit dashboard reads from.
    Pre-aggregated and denormalized for fast query performance.
    
    Materialized as TABLE (configured in dbt_project.yml)
    for dashboard speed.
*/

with

changes as (
    select * from {{ ref('int_indicators_with_changes') }}
),

signals as (
    select
        observation_month,
        sahm_rule_value,
        signal_sahm_rule,
        signal_yield_curve_inverted,
        signal_high_inflation,
        signal_low_sentiment,
        signal_fed_tightening,
        recession_signal_count,
        recession_risk_level
    from {{ ref('int_recession_signals') }}
),

metadata as (
    select * from {{ ref('stg_fred_metadata') }}
),

dashboard as (
    select
        c.observation_month,

        -- ============================================
        -- Raw indicators
        -- ============================================
        c.unemployment_rate,
        c.cpi_index,
        c.fed_funds_rate,
        c.consumer_sentiment,
        c.gdp,
        c.treasury_spread_10y2y,

        -- ============================================
        -- Derived metrics
        -- ============================================
        c.unemployment_rate_mom_change,
        c.unemployment_rate_yoy_change,
        c.unemployment_rate_3m_avg,
        c.unemployment_rate_12m_avg,
        c.inflation_rate_yoy,
        c.inflation_rate_mom_annualized,
        c.fed_funds_rate_mom_change,
        c.fed_funds_rate_yoy_change,
        c.sentiment_mom_change,
        c.sentiment_3m_avg,
        c.gdp_qoq_growth_rate,
        c.treasury_spread_3m_avg,

        -- ============================================
        -- Recession signals
        -- ============================================
        s.sahm_rule_value,
        s.signal_sahm_rule,
        s.signal_yield_curve_inverted,
        s.signal_high_inflation,
        s.signal_low_sentiment,
        s.signal_fed_tightening,
        s.recession_signal_count,
        s.recession_risk_level

    from changes c
    left join signals s
        on c.observation_month = s.observation_month
)

select * from dashboard
order by observation_month
