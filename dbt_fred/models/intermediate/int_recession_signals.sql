/*
    models/intermediate/int_recession_signals.sql

    Calculates five established recession warning signals from economic
    indicators and produces a composite risk score.

    Signals (each outputs 1 = active/warning, 0 = inactive):
      1. Sahm Rule — unemployment rises ≥0.5pp above trailing 12-month low
      2. Inverted Yield Curve — 10Y-2Y Treasury spread goes negative
      3. High Inflation — YoY CPI inflation above 5%
      4. Consumer Sentiment Collapse — Michigan index below 60
      5. Aggressive Fed Tightening — fed funds rate up ≥1pp YoY

    The composite risk level is based on the count of active signals:
      LOW (0) → MODERATE (1) → ELEVATED (2) → HIGH (3+)

    Thresholds are configurable via dbt variables (see dbt_project.yml) so
    the rules can be tuned without editing SQL.
*/

with

indicators as (
    select * from {{ ref('int_indicators_with_changes') }}
),

signals as (
    select
        observation_month,

        -- ============================================
        -- Signal 1: Sahm Rule
        -- 3-month avg unemployment rises ≥0.50pp above
        -- its trailing 12-month low. Named after economist
        -- Claudia Sahm. Historically 100% accurate at
        -- identifying recessions since 1960.
        -- ============================================
        unemployment_rate_3m_avg,
        unemployment_rate_12m_low,
        round(unemployment_rate_3m_avg - unemployment_rate_12m_low, 2)
            as sahm_rule_value,
        case
            when (unemployment_rate_3m_avg - unemployment_rate_12m_low)
                >= {{ var('sahm_rule_threshold') }}
            then 1 else 0
        end as signal_sahm_rule,

        -- ============================================
        -- Signal 2: Inverted Yield Curve
        -- When 10Y Treasury yield falls below 2Y
        -- (spread goes negative), recession often
        -- follows within 6-18 months. Has preceded every
        -- U.S. recession since 1970.
        -- ============================================
        treasury_spread_10y2y,
        case
            when treasury_spread_10y2y < 0
            then 1 else 0
        end as signal_yield_curve_inverted,

        -- ============================================
        -- Signal 3: High Inflation
        -- YoY CPI inflation above threshold often
        -- triggers aggressive Fed tightening, which
        -- historically precedes recessions.
        -- ============================================
        inflation_rate_yoy,
        case
            when inflation_rate_yoy > {{ var('high_inflation_threshold') }}
            then 1 else 0
        end as signal_high_inflation,

        -- ============================================
        -- Signal 4: Consumer Sentiment Collapse
        -- Sentiment below threshold indicates consumer
        -- retrenchment; signals reduced spending.
        -- ============================================
        consumer_sentiment,
        case
            when consumer_sentiment < {{ var('low_sentiment_threshold') }}
            then 1 else 0
        end as signal_low_sentiment,

        -- ============================================
        -- Signal 5: Aggressive Fed Tightening
        -- Fed funds rate increased ≥1pp year-over-year
        -- signals monetary tightening that historically
        -- precedes downturns.
        -- ============================================
        fed_funds_rate_yoy_change,
        case
            when fed_funds_rate_yoy_change >= {{ var('fed_tightening_threshold') }}
            then 1 else 0
        end as signal_fed_tightening

    from indicators
),

-- Compute the composite signal count ONCE, reuse it for risk level
signal_count as (
    select
        *,
        (
            coalesce(signal_sahm_rule, 0)
            + coalesce(signal_yield_curve_inverted, 0)
            + coalesce(signal_high_inflation, 0)
            + coalesce(signal_low_sentiment, 0)
            + coalesce(signal_fed_tightening, 0)
        ) as recession_signal_count
    from signals
),

risk_level as (
    select
        *,
        case
            when recession_signal_count >= 3 then 'HIGH'
            when recession_signal_count >= 2 then 'ELEVATED'
            when recession_signal_count >= 1 then 'MODERATE'
            else 'LOW'
        end as recession_risk_level
    from signal_count
)

select * from risk_level
order by observation_month
