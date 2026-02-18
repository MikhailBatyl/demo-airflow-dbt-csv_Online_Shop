{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with orders_filtered as (
    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where order_date >= (select coalesce(max(order_date), '1900-01-01')::date from {{ this }})
    {% endif %}
),

payment_one_per_order as (
    select distinct on (order_id)
        order_id,
        payment_method,
        amount as payment_amount,
        transaction_status
    from {{ ref('stg_payments') }}
    order by order_id, case when transaction_status = 'Completed' then 0 else 1 end
),

order_items_agg as (
    select
        order_id,
        sum(quantity) as items_count,
        sum(quantity * price_at_purchase) as revenue
    from {{ ref('stg_order_details') }}
    group by order_id
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    o.total_price,
    pa.payment_method,
    pa.payment_amount,
    pa.transaction_status,
    coalesce(oi.items_count, 0) as items_count,
    coalesce(oi.revenue, 0) as revenue
from orders_filtered o
left join payment_one_per_order pa on o.order_id = pa.order_id
left join order_items_agg oi on o.order_id = oi.order_id
