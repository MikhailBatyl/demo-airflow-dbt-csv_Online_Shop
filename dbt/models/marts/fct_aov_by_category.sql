with calc_category as (
    select
        pr.category as category,
        round((sum(pr.price * oi.quantity) / count(distinct o.order_id))::numeric, 2) as order_value_avg
    from {{ ref('stg_order_count_year') }} as o
    join {{ ref('stg_payments') }} as pa on o.order_id = pa.order_id
    join {{ ref('stg_order_details') }} as oi on o.order_id = oi.order_id
    join {{ ref('stg_products') }} as pr on oi.product_id = pr.product_id
    where pa.transaction_status = 'Completed'
    group by pr.category
)
select 
    category,
    order_value_avg
from calc_category
order by order_value_avg
