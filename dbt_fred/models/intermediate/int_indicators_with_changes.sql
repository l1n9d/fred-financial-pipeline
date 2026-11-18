/*
    models/intermediate/int_indicators_with_changes.sql

    Adds derived time-series metrics on top of the pivoted indicators:
    - Month-over-month changes (absolute and percentage)
    - Year-over-year changes
    - Rolling averages (3-month and 12-month)
    - CPI-derived inflation rate

    These are the metrics analysts and economists actually look at —
    raw levels alone don't tell you much without trend context.

    Missing-data policy for the unemployment rate (see the Sahm Rule
    baseline at the bottom of this model):

    UNRATE has no value for October 2025. The BLS household survey for
    that month was cancelled during the government shutdown, so FRED
    itself returns a null — the row is present in RAW.FRED_OBSERVATIONS
    with a NULL value, and staging's null filter drops it. The other
    monthly series still carry that month, so the row survives into the
    pivot with a null unemployment_rate.

    Snowflake's avg() silently skips nulls, which would quietly turn the
    three months whose window touches the hole into TWO-month averages
    wearing a "3m_avg" label. We'd rather publish nothing than publish a
    differently-defined number under the same name, so a 3-month average
    requires three real observations and is null otherwise.

    The 12-month baseline then takes the minimum of whatever averages do
    exist in its window instead of demanding all twelve. Demanding all
    twelve would blank the Sahm Rule for a full year after any hole —
    Oct 2025 through Dec 2026 in this case, including every current
    month on the dashboard — which is a much worse answer than taking a
    minimum over the nine averages we actually have. The indicator is
    therefore live again from January 2026. Months with no publishable
    value are flagged by sahm_data_complete in int_recession_signals.
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

        -- Three-month moving average, and the input to the Sahm Rule.
        -- Null unless all three months are actually present: avg() would
        -- otherwise average whatever it finds and call the result a
        -- 3-month average (see the missing-data note at the top).
        case
            when count(unemployment_rate) over (
                order by observation_month
                rows between 2 preceding and current row
            ) = 3
            then avg(unemployment_rate) over (
                order by observation_month
                rows between 2 preceding and current row
            )
        end as unemployment_rate_3m_avg,

        avg(unemployment_rate) over (
            order by observation_month
            rows between 11 preceding and current row
        ) as unemployment_rate_12m_avg,

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
),

/*
    Sahm Rule baseline: the lowest 3-month average unemployment rate over
    the PREVIOUS 12 months — months t-12 through t-1.

    Two things here are easy to get wrong, and both were wrong before:

    1. The baseline is the minimum of the three-month AVERAGES, not of the
       raw monthly rate. A raw monthly minimum is noisier and always sits
       at or below the minimum of the smoothed series, which inflates the
       indicator.
    2. The window EXCLUDES the current month. If the current month were in
       both the average and the minimum, the result could never be
       negative — but the published series does go negative during
       expansions, which is the giveaway that the window ends at t-1.

    Per Claudia Sahm's definition and FRED's SAHMREALTIME notes:
    "the minimum of the three-month averages from the previous 12 months".

    count(*) = 12 is the series warm-up guard, not a hole guard: it asks
    whether twelve prior months exist at all. A missing month would shift
    the window; the October 2025 problem is a missing VALUE in a row that
    is present, which is what the count of non-null averages handles.
*/
with_sahm_baseline as (
    select
        *,
        case
            when count(*) over (
                order by observation_month
                rows between 12 preceding and 1 preceding
            ) = 12
            and count(unemployment_rate_3m_avg) over (
                order by observation_month
                rows between 12 preceding and 1 preceding
            ) >= 1
            then min(unemployment_rate_3m_avg) over (
                order by observation_month
                rows between 12 preceding and 1 preceding
            )
        end as unemployment_rate_3m_avg_12m_low

    from with_changes
)

select * from with_sahm_baseline
order by observation_month
