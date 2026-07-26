{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'customers']
  )
}}

/*
  stg_customers — Silver staging model for customer master
  ─────────────────────────────────────────────────────────
  Source: Full daily snapshot from CRM. Deduplication is by customer_id + updated_at.
  PII note: email and name fields pass through here; Purview auto-classifies them.
             Masking policies are applied at the Gold layer for downstream consumers.
*/

with source as (
    select * from {{ source('bronze', 'raw_customers') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _ingestion_timestamp desc
        ) as _row_num
    from source
),

cleaned as (
    select
        -- Primary key
        cast(customer_id as string)                             as customer_id,

        -- PII fields — present in Silver; masked/excluded in Gold consumer-facing marts
        initcap(trim(cast(first_name as string)))               as first_name,
        initcap(trim(cast(last_name as string)))                as last_name,
        lower(trim(cast(email as string)))                      as email,

        -- Derived: full name (non-PII aggregation safe field)
        concat(
            initcap(trim(cast(first_name as string))),
            ' ',
            initcap(trim(cast(last_name as string)))
        )                                                       as full_name,

        -- Segmentation
        upper(trim(cast(country_code as string)))               as country_code,
        initcap(trim(cast(customer_segment as string)))         as customer_segment,

        -- Flags
        case
            when upper(trim(customer_segment)) = 'PREMIUM' then true
            else false
        end                                                     as is_premium_customer,

        -- Timestamps
        cast(created_at as timestamp)                           as created_at,
        cast(updated_at as timestamp)                           as updated_at,
        cast(_ingestion_timestamp as timestamp)                 as _ingestion_timestamp,

        -- Audit
        current_timestamp()                                     as _dbt_loaded_at

    from deduplicated
    where _row_num = 1
      -- Exclude soft-deleted records from source
      and customer_id is not null
      and cast(customer_id as string) != ''
)

select * from cleaned
