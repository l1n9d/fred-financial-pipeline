/*
    tests/assert_staging_to_intermediate_row_count.sql

    Validates that the intermediate pivoted table has a reasonable
    row count relative to the staging observations.

    Logic:
    - Staging has one row per (series_id, date) — many rows per month
    - Intermediate pivoted has one row per month — much fewer rows
    - The pivoted table should have roughly as many rows as the number
      of distinct months in staging for any single monthly series
    - If pivoted has ZERO rows → the join or pivot logic is broken
    - If pivoted has MORE rows than distinct staging months → fanout problem

    dbt convention: this query should return 0 rows to pass.
*/

with

staging_month_count as (
    select count(distinct observation_date) as staging_months
    from {{ ref('stg_fred_observations') }}
    where series_id = 'UNRATE'  -- monthly series, one per month
),

intermediate_row_count as (
    select count(*) as pivoted_rows
    from {{ ref('int_economic_indicators_pivoted') }}
)

select
    s.staging_months,
    i.pivoted_rows,
    i.pivoted_rows - s.staging_months as row_difference
from staging_month_count s
cross join intermediate_row_count i
where i.pivoted_rows = 0                              -- no rows = broken
   or i.pivoted_rows > s.staging_months * 1.1          -- >10% more = fanout
   or i.pivoted_rows < s.staging_months * 0.5          -- >50% fewer = filtering
