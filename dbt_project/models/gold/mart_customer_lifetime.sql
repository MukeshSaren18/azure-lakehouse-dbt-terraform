{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    tags=['gold', 'mart', 'clv', 'ml_feature_ready'],
    post_hook="OPTIMIZE {{ this }} ZORDER BY (customer_segment, customer_status)"
  )
}}

/*
  mart_customer_lifetime — One Big Table (OBT) for customer analytics + ML features
  ──────────────────────────────────────────────────────────────────────────────────
  Grain: one row per customer (latest state + rolling aggregates).
  Designed for:
    - BI / Power BI reporting
    - ML feature engineering (feeds directly into the Real-Time Feature Store)
    - Customer segmentation and churn modelling

  Rolling windows computed: 7d, 30d, 90d, 365d, lifetime
*/

with customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
    where is_fulfilled = true   -- count only completed orders for CLV
),

-- Rolling order aggregations
order_aggregates as (
    select
        customer_id,

        -- Volume metrics
        count(distinct order_id)                                        as lifetime_orders,
        sum(net_revenue_eur)                                            as lifetime_revenue_eur,
        avg(net_revenue_eur)                                            as avg_order_value_eur,

        -- Recency windows (for churn / engagement scoring)
        count(distinct case when order_date >= current_date - 7   then order_id end) as orders_last_7d,
        count(distinct case when order_date >= current_date - 30  then order_id end) as orders_last_30d,
        count(distinct case when order_date >= current_date - 90  then order_id end) as orders_last_90d,
        count(distinct case when order_date >= current_date - 365 then order_id end) as orders_last_365d,

        sum(case when order_date >= current_date - 30  then net_revenue_eur else 0 end) as revenue_last_30d,
        sum(case when order_date >= current_date - 90  then net_revenue_eur else 0 end) as revenue_last_90d,
        sum(case when order_date >= current_date - 365 then net_revenue_eur else 0 end) as revenue_last_365d,

        -- Purchase behaviour
        mode() within group (order by primary_payment_method)           as preferred_payment_method,
        mode() within group (order by order_size_band)                  as typical_order_size,
        count(distinct case when has_chargeback then order_id end)      as chargeback_count,
        sum(refunded_amount_eur)                                        as total_refunded_eur,

        -- Timestamps
        min(order_timestamp)                                            as first_order_at,
        max(order_timestamp)                                            as last_order_at,
        datediff(day, min(order_date), max(order_date))                 as customer_tenure_days

    from orders
    group by customer_id
),

final as (
    select
        -- Customer identity
        c.customer_id,
        c.customer_sk,
        c.country_code,
        c.customer_segment,
        c.is_premium_customer,
        c.customer_status,

        -- Lifetime metrics
        coalesce(o.lifetime_orders, 0)              as lifetime_orders,
        coalesce(o.lifetime_revenue_eur, 0)         as lifetime_revenue_eur,
        coalesce(o.avg_order_value_eur, 0)          as avg_order_value_eur,

        -- Rolling window metrics (ML feature candidates)
        coalesce(o.orders_last_7d, 0)               as orders_last_7d,
        coalesce(o.orders_last_30d, 0)              as orders_last_30d,
        coalesce(o.orders_last_90d, 0)              as orders_last_90d,
        coalesce(o.orders_last_365d, 0)             as orders_last_365d,
        coalesce(o.revenue_last_30d, 0)             as revenue_last_30d,
        coalesce(o.revenue_last_90d, 0)             as revenue_last_90d,
        coalesce(o.revenue_last_365d, 0)            as revenue_last_365d,

        -- Risk signals
        coalesce(o.chargeback_count, 0)             as chargeback_count,
        coalesce(o.total_refunded_eur, 0)           as total_refunded_eur,
        case when coalesce(o.chargeback_count, 0) > 0 then true else false end as is_high_risk,

        -- Engagement signals
        o.preferred_payment_method,
        o.typical_order_size,
        coalesce(o.customer_tenure_days, 0)         as customer_tenure_days,

        -- Recency (days since last order — key churn feature)
        case
            when o.last_order_at is not null
            then datediff(day, date(o.last_order_at), current_date)
            else null
        end                                         as days_since_last_order,

        -- Timestamps
        o.first_order_at,
        o.last_order_at,
        c.created_at                                as customer_created_at,

        -- Audit
        current_timestamp()                         as _dbt_loaded_at

    from customers c
    left join order_aggregates o on c.customer_id = o.customer_id

    {% if is_incremental() %}
    -- On incremental runs, only update customers with recent order activity
    where c.customer_id in (
        select distinct customer_id from orders
        where order_timestamp >= dateadd(day, -{{ var('incremental_lookback_days') }},
              (select max(_dbt_loaded_at) from {{ this }}))
    )
    {% endif %}
)

select * from final
