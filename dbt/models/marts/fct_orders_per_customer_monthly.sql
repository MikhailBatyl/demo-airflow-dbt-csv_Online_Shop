SELECT
    DATE_TRUNC('month', o.order_date)::date as month,
    COUNT(DISTINCT o.order_id)::float / COUNT(DISTINCT o.customer_id) as orders_per_customer
from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_customers') }} as cu on o.customer_id = cu.customer_id
join {{ ref('stg_payments') }} as pa on o.order_id = pa.order_id 
where pa.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month
