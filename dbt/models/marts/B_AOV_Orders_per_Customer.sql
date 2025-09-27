select 
	count(distinct o.order_id)::float/count(distinct cu.customer_id) as order_per_customer
from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_customers') }} as cu on o.customer_id = cu.customer_id
join {{ ref('stg_payments') }} as pa on o.order_id=pa.order_id 
where pa.transaction_status = 'Completed'

