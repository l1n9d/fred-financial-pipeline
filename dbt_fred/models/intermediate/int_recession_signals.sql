/*
    models/intermediate/int_recession_signals.sql

    Calculates recession warning signals from economic indicators.
    These are established economic rules used by the Federal Reserve
    and economists to assess recession risk.

    Signals:
    1. Sahm Rule — unemployment rate rises 0.5+ pp above its 12-month low
    2. Inverted Yield Curve — 10Y-2Y Treasury spread goes negative
    3. Inflation Acceleration — YoY inflation rising above 5%
    4. Sentiment Collapse — consumer sentiment drops below 60
    5. Fed Tightening — fed funds rate increased YoY by 1+ pp

    Each signal outputs 1 (active/warning) or 0 (inactive).
    A composite score counts how many signals are active.
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
        -- Triggered when 3-month average unemployment rate
        -- rises 0.50+ pp above its trailing 12-month low.
        -- Named after economist Claudia Sahm.
        -- Historically 100% accurate at identifying recessions.
        -- ============================================
        unemployment_rate_3m_avg,
        unemployment_rate_12m_low,
        round(unemployment_rate_3m_avg - unemployment_rate_12m_low, 2)
            as sahm_rule_value,
        case
            when (unemployment_rate_3m_avg - unemployment_rate_12m_low) >= 0.50
            then 1 else 0
        end as signal_sahm_rule,

        -- ============================================
        -- Signal 2: Inverted Yield Curve
        -- When 10Y Treasury yield falls below 2Y yield
        -- (spread goes negative), recession often follows
        -- within 6-18 months. One of the most reliable
        -- leading indicators.
        -- ============================================
        treasury_spread_10y2y,
        case
            when treasury_spread_10y2y < 0
            then 1 else 0
        end as signal_yield_curve_inverted,

        -- ============================================
        -- Signal 3: High Inflation
        -- YoY inflation above 5% often precedes Fed
        -- tightening, which can trigger a recession.
        -- ============================================
        inflation_rate_yoy,
        case
            when inflation_rate_yoy > 5.0
            then 1 else 0
        end as signal_high_inflation,

        -- ============================================
        -- Signal 4: Consumer Sentiment Collapse
        -- Sentiment below 60 indicates significant
        -- pessimism about economic outlook.
        -- ============================================
        consumer_sentiment,
        case
            when consumer_sentiment < 60
            then 1 else 0
        end as signal_low_sentiment,

        -- ============================================
        -- Signal 5: Aggressive Fed Tightening
        -- Fed funds rate increased by 1+ percentage
        -- points year-over-year signals aggressive
        -- monetary tightening.
        -- ============================================
        fed_funds_rate_yoy_change,
        case
            when fed_funds_rate_yoy_change >= 1.0
            then 1 else 0
        end as signal_fed_tightening

    from indicators
),

with_composite as (
    select
        *,

        -- Composite score: how many signals are currently active (0-5)
        (
            coalesce(signal_sahm_rule, 0)
            + coalesce(signal_yield_curve_inverted, 0)
            + coalesce(signal_high_inflation, 0)
            + coalesce(signal_low_sentiment, 0)
            + coalesce(signal_fed_tightening, 0)
        ) as recession_signal_count,

        -- Risk level based on signal count
        case
            when (
                coalesce(signal_sahm_rule, 0)
                + coalesce(signal_yield_curve_inverted, 0)
                + coalesce(signal_high_inflation, 0)
                + coalesce(signal_low_sentiment, 0)
                + coalesce(signal_fed_tightening, 0)
            ) >= 3 then 'HIGH'
            when (
                coalesce(signal_sahm_rule, 0)
                + coalesce(signal_yield_curve_inverted, 0)
                + coalesce(signal_high_inflation, 0)
                + coalesce(signal_low_sentiment, 0)
                + coalesce(signal_fed_tightening, 0)
            ) >= 2 then 'ELEVATED'
            when (
                coalesce(signal_sahm_rule, 0)
                + coalesce(signal_yield_curve_inverted, 0)
                + coalesce(signal_high_inflation, 0)
                + coalesce(signal_low_sentiment, 0)
                + coalesce(signal_fed_tightening, 0)
            ) >= 1 then 'MODERATE'
            else 'LOW'
        end as recession_risk_level

    from signals
)

select * from with_composite
order by observation_month
