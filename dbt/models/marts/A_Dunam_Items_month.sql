select 
	date_part('month', o.order_date) as month,	
	round(avg(items_per_order)) as avg_items_per_order
from (select 
			o.order_id,
			sum(oi.quantity) as items_per_order
	 from {{ ref('stg_order_count_year') }} as o
	 join {{ ref('stg_order_items') }} as oi on o.order_id=oi.order_id
	 join {{ ref('stg_payments') }} as p on o.order_id=p.order_id
	 where p.transaction_status = 'Completed'
	 group by o.order_id) as tq	
join {{ ref('stg_order_count_year') }} as o on tq.order_id = o.order_id
group by month
order by month























