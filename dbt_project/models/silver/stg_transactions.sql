{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'transactions']
  )
}}

/*
  stg_transactions — Silver staging model for payment transactions
  ─────────────────────────────────────────────────────────────────
  High-volume table partitioned by event_date in Bronze.
  Incremental-safe: this view always reads full Bronze; Gold fct_orders
  applies the incremental filter to limit what gets merged downstream.
*/

with source as (
    select * from {{ source('bronze', 'raw_transactions') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by _ingestion_timestamp desc
        ) as _row_num
    from source
),

cleaned as (
    select
        -- Primary key
        cast(transaction_id as string)                              as transaction_id,

        -- Foreign keys
        cast(order_id as string)                                    as order_id,

        -- Payment details
        upper(trim(cast(payment_method as string)))                 as payment_method,
        round(cast(transaction_amount_eur as decimal(18, 2)), 2)    as transaction_amount_eur,
        upper(trim(cast(transaction_status as string)))             as transaction_status,

        -- Flags
        case
            when upper(trim(transaction_status)) = 'COMPLETED' then true
            else false
        end                                                         as is_successful,

        case
            when upper(trim(payment_method)) in ('CARD', 'APPLE_PAY', 'GOOGLE_PAY')
            then true else false
        end                                                         as is_digital_payment,

        -- Timestamps
        cast(transaction_timestamp as timestamp)                    as transaction_timestamp,
        date(cast(transaction_timestamp as timestamp))              as transaction_date,

        -- Audit
        cast(_ingestion_timestamp as timestamp)                     as _ingestion_timestamp,
        current_timestamp()                                         as _dbt_loaded_at

    from deduplicated
    where _row_num = 1
      and transaction_id is not null
      and cast(transaction_amount_eur as decimal(18, 2)) > 0
)

select * from cleaned
