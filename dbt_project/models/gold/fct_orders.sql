{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    tags=['gold', 'fact', 'orders'],
    -- Partition by order_date for query pruning; Z-Order on customer_id + status
    partition_by={'field': 'order_date', 'data_type': 'date'},
    post_hook="OPTIMIZE {{ this }} ZORDER BY (customer_id, order_status)"
  )
}}

/*
  fct_orders — Gold fact table for order events
  ──────────────────────────────────────────────
  Incremental merge strategy: only processes orders created/modified
  within the lookback window, then merges on order_id.

  Joins: stg_orders + stg_transactions + dim_customers (for customer_sk)
  Grain: one row per order_id (latest state)
*/

with orders as (
    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    -- Only process records newer than last run minus lookback buffer
    where order_timestamp >= dateadd(
        day,
        -{{ var('incremental_lookback_days') }},
        (select max(order_timestamp) from {{ this }})
    )
    {% endif %}
),

transactions as (
    select
        order_id,
        count(distinct transaction_id)                              as transaction_count,
        sum(case when is_successful then transaction_amount_eur
                 else 0 end)                                        as paid_amount_eur,
        sum(case when transaction_status = 'REFUNDED'
                 then transaction_amount_eur else 0 end)            as refunded_amount_eur,
        max(case when is_successful then transaction_timestamp end) as last_paid_at,
        bool_or(transaction_status = 'CHARGEBACK')                  as has_chargeback,
        mode() within group (order by payment_method)               as primary_payment_method
    from {{ ref('stg_transactions') }}
    group by order_id
),

customers as (
    select customer_id, customer_sk, customer_segment, is_premium_customer
    from {{ ref('dim_customers') }}
),

final as (
    select
        -- Keys
        o.order_id,
        c.customer_sk,
        o.customer_id,

        -- Dates (for partitioning + time-intelligence queries)
        o.order_date,
        o.order_timestamp,

        -- Order attributes
        o.order_status,
        o.order_size_band,
        o.order_amount_eur,

        -- Customer attributes at order time (denormalised for query performance)
        c.customer_segment,
        c.is_premium_customer,

        -- Payment enrichment
        coalesce(t.transaction_count, 0)        as transaction_count,
        coalesce(t.paid_amount_eur, 0)          as paid_amount_eur,
        coalesce(t.refunded_amount_eur, 0)      as refunded_amount_eur,
        t.last_paid_at,
        coalesce(t.has_chargeback, false)       as has_chargeback,
        t.primary_payment_method,

        -- Derived: net revenue (order - refunds)
        o.order_amount_eur - coalesce(t.refunded_amount_eur, 0) as net_revenue_eur,

        -- Fulfilment flag
        o.order_status = 'DELIVERED'            as is_fulfilled,

        -- Audit
        o._ingestion_timestamp,
        current_timestamp()                     as _dbt_loaded_at

    from orders o
    left join transactions t on o.order_id = t.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final
