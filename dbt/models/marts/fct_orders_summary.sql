select 
    o.customer_id,
    count(*) as orders_count,
    sum(od.quantity * od.price_at_purchase) as total_sales
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_details') }} od
    on o.order_id = od.order_id
group by o.customer_id
