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
        sahm_rule_value_effective,
        sahm_rule_value_as_of_month,
        sahm_data_complete,
        treasury_spread_10y2y_effective,
        treasury_spread_10y2y_as_of_month,
        treasury_spread_data_complete,
        inflation_rate_yoy_effective,
        inflation_rate_yoy_as_of_month,
        inflation_data_complete,
        consumer_sentiment_effective,
        consumer_sentiment_as_of_month,
        consumer_sentiment_data_complete,
        fed_funds_rate_yoy_change_effective,
        fed_funds_rate_yoy_change_as_of_month,
        fed_funds_data_complete,
        signal_sahm_rule,
        signal_yield_curve_inverted,
        signal_high_inflation,
        signal_low_sentiment,
        signal_fed_tightening,
        recession_signal_count,
        recession_signal_measured_count,
        recession_signal_unknown_count,
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
        s.recession_risk_level,

        -- ============================================
        -- Signal provenance: the value each signal was actually
        -- decided on, the month it was published for, and whether
        -- it was fresh enough to count as a measurement.
        -- See the missing-data policy in int_recession_signals.sql.
        -- ============================================
        s.sahm_rule_value_effective,
        s.sahm_rule_value_as_of_month,
        s.sahm_data_complete,
        s.treasury_spread_10y2y_effective,
        s.treasury_spread_10y2y_as_of_month,
        s.treasury_spread_data_complete,
        s.inflation_rate_yoy_effective,
        s.inflation_rate_yoy_as_of_month,
        s.inflation_data_complete,
        s.consumer_sentiment_effective,
        s.consumer_sentiment_as_of_month,
        s.consumer_sentiment_data_complete,
        s.fed_funds_rate_yoy_change_effective,
        s.fed_funds_rate_yoy_change_as_of_month,
        s.fed_funds_data_complete,
        s.recession_signal_measured_count,
        s.recession_signal_unknown_count

    from changes c
    left join signals s
        on c.observation_month = s.observation_month
)

select * from dashboard
order by observation_month
