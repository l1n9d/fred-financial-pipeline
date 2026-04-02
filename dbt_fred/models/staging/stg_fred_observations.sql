/*
    models/staging/stg_fred_observations.sql

    Staging model for FRED observations.
    
    What this does:
    1. Reads from the raw fred_observations table
    2. Deduplicates: if the same (series_id, date) was loaded twice,
       keeps only the most recently ingested row
    3. Adds a surrogate key for downstream joins
    4. Filters out rows with NULL values (FRED returns '.' for missing)

    Materialized as a VIEW (configured in dbt_project.yml)
    so it always reflects the latest raw data.
*/

with

source as (
    select * from {{ source('fred_raw', 'fred_observations') }}
),

-- Deduplicate: keep only the latest ingestion per series + date
deduplicated as (
    select
        series_id,
        observation_date,
        value,
        ingested_at,
        source_url,
        row_number() over (
            partition by series_id, observation_date
            order by ingested_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        -- Surrogate key: unique identifier for each observation
        {{ dbt_utils.generate_surrogate_key(['series_id', 'observation_date']) }} as observation_id,
        
        series_id,
        observation_date,
        value,
        ingested_at,
        source_url
    from deduplicated
    where row_num = 1          -- keep only latest per series + date
      and value is not null    -- remove missing observations
)

select * from cleaned