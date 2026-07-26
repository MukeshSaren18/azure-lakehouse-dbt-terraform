{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'orders']
  )
}}

/*
  stg_orders — Silver staging model for order events
  ──────────────────────────────────────────────────
  Responsibilities:
    1. Deduplicate raw Bronze rows (source may emit duplicates on retries)
    2. Cast all columns to canonical types
    3. Rename to snake_case standard
    4. Derive simple calculated fields (no cross-table joins at this layer)
    5. Apply NULL handling and accepted-value guards

  Deduplication strategy:
    Use ROW_NUMBER() partitioned by order_id, ordered by _ingestion_timestamp DESC
    so the latest ingested event wins (last-write-wins within Bronze).
*/

with source as (
    select * from {{ source('bronze', 'raw_orders') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id
            order by _ingestion_timestamp desc
        ) as _row_num
    from source
),

cleaned as (
    select
        -- Primary key
        cast(order_id as string)                                as order_id,

        -- Foreign keys
        cast(customer_id as string)                             as customer_id,

        -- Measures — cast and round to 2dp for EUR
        round(cast(order_amount_eur as decimal(18, 2)), 2)      as order_amount_eur,

        -- Status — upper and trim to normalise source inconsistencies
        upper(trim(order_status))                               as order_status,

        -- Timestamps — cast to UTC timestamp
        cast(order_timestamp as timestamp)                      as order_timestamp,
        date(cast(order_timestamp as timestamp))                as order_date,

        -- Derived: bucket order size for segmentation
        case
            when cast(order_amount_eur as decimal(18, 2)) < 50    then 'small'
            when cast(order_amount_eur as decimal(18, 2)) < 500   then 'medium'
            when cast(order_amount_eur as decimal(18, 2)) < 5000  then 'large'
            else 'enterprise'
        end                                                     as order_size_band,

        -- Audit columns
        cast(_ingestion_timestamp as timestamp)                 as _ingestion_timestamp,
        cast(_source_file as string)                            as _source_file,
        current_timestamp()                                     as _dbt_loaded_at

    from deduplicated
    where _row_num = 1
      -- Exclude test/seed records from source system
      and upper(trim(order_status)) not in ('TEST', 'DUMMY', 'CANCELLED_TEST')
      -- Guard against future-dated records (data quality)
      and cast(order_timestamp as timestamp) <= current_timestamp()
)

select * from cleaned
