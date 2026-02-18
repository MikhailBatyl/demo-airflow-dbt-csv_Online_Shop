select 
	distinct(o.order_id) as order_id,
	o.order_date as order_date,
	o.total_price as total_price,
	pa.amount as amount,
	oi.quantity as quantity, 
	oi.price_at_purchase as price_at_purchase,
	pa.transaction_status as transaction_status
from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_payments') }} as pa on o.order_id = pa.order_id
join {{ ref('stg_order_details') }} as oi on o.order_id = oi.order_id
where pa.transaction_status = 'Completed'
