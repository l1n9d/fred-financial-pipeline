/*
    tests/assert_sahm_rule_value_in_range.sql

    Validates that the Sahm Rule value is within a reasonable range.
    
    - Should never be negative (it's a difference between a 3-month avg
      and a 12-month low — the avg can't be below the min)
    - Should never exceed 10 (even during the Great Depression, 
      unemployment didn't spike that fast in a 12-month window)
    
    If this test fails, the rolling average or window logic in
    int_recession_signals.sql is broken.

    dbt convention: this query should return 0 rows to pass.
*/

select
    observation_month,
    sahm_rule_value
from {{ ref('int_recession_signals') }}
where sahm_rule_value is not null
  and (sahm_rule_value < 0 or sahm_rule_value > 10)
