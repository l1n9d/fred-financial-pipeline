/*
    tests/assert_sahm_rule_value_in_range.sql

    Validates that the Sahm Rule value is within a reasonable range.

    - Moderate negatives are CORRECT, not a bug. The baseline is the
      lowest 3-month average of the previous 12 months and excludes the
      current month, so when unemployment drifts down the current average
      sits below that baseline. The published FRED series behaves the same
      way. Our history reaches -0.40. A value below -2 would mean
      unemployment fell 2pp in under a year without the baseline
      following it, which in practice means the window is inverted or the
      baseline is being taken over the wrong series.
    - Should never exceed 10 (even during the Great Depression,
      unemployment didn't spike that fast in a 12-month window). The COVID
      peak, the largest in our data, is 9.43.

    If this test fails, look at the window logic in
    int_indicators_with_changes.sql — that is where the 3-month average
    and the 12-month baseline are computed. int_recession_signals.sql only
    subtracts them and applies the threshold.

    dbt convention: this query should return 0 rows to pass.
*/

select
    observation_month,
    sahm_rule_value
from {{ ref('int_recession_signals') }}
where sahm_rule_value is not null
  and (sahm_rule_value < -2 or sahm_rule_value > 10)
