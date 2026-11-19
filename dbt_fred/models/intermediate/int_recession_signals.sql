/*
    models/intermediate/int_recession_signals.sql

    Calculates five established recession warning signals from economic
    indicators and produces a composite risk score.

    Signals (each outputs 1 = active/warning, 0 = not active — where
    "not active" means either measured-and-below-threshold or unknown,
    which is what the accompanying *_data_complete flag separates):
      1. Sahm Rule — 3-month avg unemployment rises ≥0.5pp above the
         lowest 3-month average of the PREVIOUS 12 months
      2. Inverted Yield Curve — 10Y-2Y Treasury spread goes negative
      3. High Inflation — YoY CPI inflation above 5%
      4. Consumer Sentiment Collapse — Michigan index below 60
      5. Aggressive Fed Tightening — fed funds rate up ≥1pp YoY

    The composite risk level is based on the count of active signals:
      LOW (0) → MODERATE (1) → ELEVATED (2) → HIGH (3+)
    unless too few signals were measurable, in which case no level is
    published at all: INSUFFICIENT DATA.

    Thresholds are configurable via dbt variables (see dbt_project.yml) so
    the rules can be tuned without editing SQL.

    ================================================================
    Missing-data policy: latest available value, with a staleness limit
    ================================================================

    These five series do not publish on the same calendar. UMCSENT lags by
    a month, CPI by two weeks, T10Y2Y is daily. A signal written as
    "case when x > threshold then 1 else 0 end" answers a NULL input with
    0 — so a month nobody has measured yet is published as "not
    triggered", indistinguishable from a real all-clear.

    That is not a hypothetical. In September 2026 consumer sentiment was
    48.1, below the threshold, so the signal was active and the composite
    read MODERATE. In October UMCSENT had not published, the signal
    silently became 0, and the dashboard reported LOW — a fall in
    reported risk caused purely by data going missing.

    So each signal now reads its most recent published value and carries
    the month that value came from. Once the value is older than
    signal_staleness_months it stops counting as a measurement: the flag
    goes false, the signal cannot trigger, and the composite says how many
    of the five were actually measured.

    Two deliberate limits on this:

    1. It carries the SIGNAL INPUT, never the underlying series. Filling
       the October 2025 hole in cpi_index would make October 2026's YoY
       compute against a fabricated baseline and publish it as a real
       measurement. Carrying inflation_rate_yoy forward instead labels a
       stale number as stale. Nothing upstream of this model is touched.
    2. Raw columns keep their own names and their own values. The carried
       values live in *_effective, so anything reading sahm_rule_value or
       consumer_sentiment still sees real observations only.
*/

with

indicators as (
    select * from {{ ref('int_indicators_with_changes') }}
),

-- Compute the published Sahm value ONCE so the signal is decided on the
-- same number the dashboard displays. Rounding only at display time let
-- months show "0.50" next to a 0 signal, because the comparison saw
-- 0.4951 while the reader saw 0.50.
sahm as (
    select
        *,
        round(unemployment_rate_3m_avg - unemployment_rate_3m_avg_12m_low, 2)
            as sahm_rule_value
    from indicators
),

/*
    Carry each signal input forward to its most recent published value,
    alongside the month that value was published for.

    last_value(... ignore nulls) over an unbounded-preceding window is
    "the newest non-null at or before this row". The paired as-of month
    uses the same window over the month of each non-null reading, so the
    two always describe the same observation.
*/
carried as (
    select
        *,

        -- Signal 1 input: Sahm Rule value
        last_value(sahm_rule_value ignore nulls) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as sahm_rule_value_effective,
        last_value(
            case when sahm_rule_value is not null then observation_month end
            ignore nulls
        ) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as sahm_rule_value_as_of_month,

        -- Signal 2 input: 10Y-2Y Treasury spread
        last_value(treasury_spread_10y2y ignore nulls) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as treasury_spread_10y2y_effective,
        last_value(
            case when treasury_spread_10y2y is not null then observation_month end
            ignore nulls
        ) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as treasury_spread_10y2y_as_of_month,

        -- Signal 3 input: YoY CPI inflation (never cpi_index itself)
        last_value(inflation_rate_yoy ignore nulls) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as inflation_rate_yoy_effective,
        last_value(
            case when inflation_rate_yoy is not null then observation_month end
            ignore nulls
        ) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as inflation_rate_yoy_as_of_month,

        -- Signal 4 input: Michigan consumer sentiment
        last_value(consumer_sentiment ignore nulls) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as consumer_sentiment_effective,
        last_value(
            case when consumer_sentiment is not null then observation_month end
            ignore nulls
        ) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as consumer_sentiment_as_of_month,

        -- Signal 5 input: YoY change in the fed funds rate
        last_value(fed_funds_rate_yoy_change ignore nulls) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as fed_funds_rate_yoy_change_effective,
        last_value(
            case when fed_funds_rate_yoy_change is not null then observation_month end
            ignore nulls
        ) over (
            order by observation_month
            rows between unbounded preceding and current row
        ) as fed_funds_rate_yoy_change_as_of_month

    from sahm
),

/*
    A carried value counts as a measurement while it is at most
    signal_staleness_months old. coalesce(..., false) is what handles the
    series warm-up: before any value has ever published, the as-of month
    is null, datediff returns null, and the comparison would otherwise be
    null rather than "unknown".
*/
completeness as (
    select
        *,
        coalesce(
            datediff('month', sahm_rule_value_as_of_month, observation_month)
                <= {{ var('signal_staleness_months') }},
            false
        ) as sahm_data_complete,

        coalesce(
            datediff('month', treasury_spread_10y2y_as_of_month, observation_month)
                <= {{ var('signal_staleness_months') }},
            false
        ) as treasury_spread_data_complete,

        coalesce(
            datediff('month', inflation_rate_yoy_as_of_month, observation_month)
                <= {{ var('signal_staleness_months') }},
            false
        ) as inflation_data_complete,

        coalesce(
            datediff('month', consumer_sentiment_as_of_month, observation_month)
                <= {{ var('signal_staleness_months') }},
            false
        ) as consumer_sentiment_data_complete,

        coalesce(
            datediff('month', fed_funds_rate_yoy_change_as_of_month, observation_month)
                <= {{ var('signal_staleness_months') }},
            false
        ) as fed_funds_data_complete

    from carried
),

signals as (
    select
        observation_month,

        -- ============================================
        -- Signal 1: Sahm Rule
        -- 3-month avg unemployment rises ≥0.50pp above the
        -- lowest 3-month average of the PREVIOUS 12 months
        -- (t-12..t-1, excluding the current month). Named
        -- after economist Claudia Sahm. Historically 100%
        -- accurate at identifying recessions since 1960.
        --
        -- The value is legitimately negative when unemployment
        -- is drifting down, and null when the underlying data
        -- has a hole — sahm_data_complete tells those apart
        -- from a genuine "not triggered".
        -- ============================================
        unemployment_rate_3m_avg,
        unemployment_rate_3m_avg_12m_low,
        sahm_rule_value,
        sahm_rule_value_effective,
        sahm_rule_value_as_of_month,
        sahm_data_complete,
        case
            when sahm_data_complete
             and sahm_rule_value_effective >= {{ var('sahm_rule_threshold') }}
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
        treasury_spread_10y2y_effective,
        treasury_spread_10y2y_as_of_month,
        treasury_spread_data_complete,
        case
            when treasury_spread_data_complete
             and treasury_spread_10y2y_effective < 0
            then 1 else 0
        end as signal_yield_curve_inverted,

        -- ============================================
        -- Signal 3: High Inflation
        -- YoY CPI inflation above threshold often
        -- triggers aggressive Fed tightening, which
        -- historically precedes recessions.
        -- ============================================
        inflation_rate_yoy,
        inflation_rate_yoy_effective,
        inflation_rate_yoy_as_of_month,
        inflation_data_complete,
        case
            when inflation_data_complete
             and inflation_rate_yoy_effective > {{ var('high_inflation_threshold') }}
            then 1 else 0
        end as signal_high_inflation,

        -- ============================================
        -- Signal 4: Consumer Sentiment Collapse
        -- Sentiment below threshold indicates consumer
        -- retrenchment; signals reduced spending.
        -- ============================================
        consumer_sentiment,
        consumer_sentiment_effective,
        consumer_sentiment_as_of_month,
        consumer_sentiment_data_complete,
        case
            when consumer_sentiment_data_complete
             and consumer_sentiment_effective < {{ var('low_sentiment_threshold') }}
            then 1 else 0
        end as signal_low_sentiment,

        -- ============================================
        -- Signal 5: Aggressive Fed Tightening
        -- Fed funds rate increased ≥1pp year-over-year
        -- signals monetary tightening that historically
        -- precedes downturns.
        -- ============================================
        fed_funds_rate_yoy_change,
        fed_funds_rate_yoy_change_effective,
        fed_funds_rate_yoy_change_as_of_month,
        fed_funds_data_complete,
        case
            when fed_funds_data_complete
             and fed_funds_rate_yoy_change_effective >= {{ var('fed_tightening_threshold') }}
            then 1 else 0
        end as signal_fed_tightening

    from completeness
),

-- Compute the composite counts ONCE, reuse them for the risk level.
-- recession_signal_measured_count is the single value the risk rule, the
-- marts and the dashboard header all read: how many of the five were
-- backed by a value fresh enough to count.
signal_count as (
    select
        *,
        (
            coalesce(signal_sahm_rule, 0)
            + coalesce(signal_yield_curve_inverted, 0)
            + coalesce(signal_high_inflation, 0)
            + coalesce(signal_low_sentiment, 0)
            + coalesce(signal_fed_tightening, 0)
        ) as recession_signal_count,
        (
            case when sahm_data_complete then 1 else 0 end
            + case when treasury_spread_data_complete then 1 else 0 end
            + case when inflation_data_complete then 1 else 0 end
            + case when consumer_sentiment_data_complete then 1 else 0 end
            + case when fed_funds_data_complete then 1 else 0 end
        ) as recession_signal_measured_count
    from signals
),

unknown_count as (
    select
        *,
        5 - recession_signal_measured_count as recession_signal_unknown_count
    from signal_count
),

risk_level as (
    select
        *,
        case
            -- Too little measured to say anything: withhold the level
            -- rather than report a confident LOW built out of silence.
            when recession_signal_measured_count < {{ var('min_measured_signals') }}
                then 'INSUFFICIENT DATA'
            when recession_signal_count >= 3 then 'HIGH'
            when recession_signal_count >= 2 then 'ELEVATED'
            when recession_signal_count >= 1 then 'MODERATE'
            else 'LOW'
        end as recession_risk_level
    from unknown_count
)

select * from risk_level
order by observation_month
