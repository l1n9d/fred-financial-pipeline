/*
    models/staging/stg_fred_metadata.sql

    Staging model for FRED series metadata.
    
    What this does:
    1. Reads from the raw fred_series_metadata table
    2. Deduplicates on series_id (keeps latest ingestion)
    3. Cleans up column names and types
*/

with

source as (
    select * from {{ source('fred_raw', 'fred_series_metadata') }}
),

deduplicated as (
    select
        series_id,
        title,
        frequency,
        units,
        seasonal_adj,
        last_updated,
        ingested_at,
        row_number() over (
            partition by series_id
            order by ingested_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        series_id,
        title               as series_title,
        frequency           as data_frequency,
        units               as measurement_units,
        seasonal_adj        as seasonal_adjustment,
        last_updated        as fred_last_updated,
        ingested_at
    from deduplicated
    where row_num = 1
)

select * from cleaned