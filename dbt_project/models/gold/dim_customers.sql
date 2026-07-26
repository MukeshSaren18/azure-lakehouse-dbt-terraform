{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'customers'],
    -- Delta Lake optimisation: Z-Order on customer_id for point lookups
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (customer_id)",
      "ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS"
    ]
  )
}}

/*
  dim_customers — Gold customer dimension
  ────────────────────────────────────────
  SCD Type 1 (latest values overwrite).
  PII masking: email replaced with SHA-256 hash for non-privileged consumers.
  Premium segment flag surfaced for segmentation queries.

  Downstream: fct_orders, mart_customer_lifetime
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate order stats to enrich the dimension
order_stats as (
    select
        customer_id,
        count(distinct order_id)            as lifetime_order_count,
        sum(order_amount_eur)               as lifetime_order_value_eur,
        min(order_timestamp)                as first_order_at,
        max(order_timestamp)                as last_order_at,
        count(distinct order_date)          as distinct_order_days
    from {{ ref('stg_orders') }}
    where order_status != 'CANCELLED'
    group by customer_id
),

enriched as (
    select
        -- Surrogate key (dbt_utils generates a deterministic hash)
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }}  as customer_sk,

        -- Natural key
        c.customer_id,

        -- Attributes — PII masked for non-privileged downstream consumers
        c.full_name,
        sha2(c.email, 256)                                         as email_hash,  -- PII masked
        c.country_code,
        c.customer_segment,
        c.is_premium_customer,

        -- Enriched from order stats
        coalesce(o.lifetime_order_count, 0)                        as lifetime_order_count,
        coalesce(o.lifetime_order_value_eur, 0)                    as lifetime_order_value_eur,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.distinct_order_days, 0)                         as distinct_order_days,

        -- Derived customer health flag
        case
            when o.last_order_at >= dateadd(day, -90, current_timestamp()) then 'active'
            when o.last_order_at >= dateadd(day, -365, current_timestamp()) then 'lapsing'
            when o.last_order_at is not null then 'churned'
            else 'never_ordered'
        end                                                        as customer_status,

        -- Audit
        c.created_at,
        c.updated_at,
        current_timestamp()                                        as _dbt_loaded_at

    from customers c
    left join order_stats o on c.customer_id = o.customer_id
)

select * from enriched
