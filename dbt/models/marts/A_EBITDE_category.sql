select
	date_part('year', o.order_date) as year,
	date_part('month', o.order_date) as month,
  	pr.category,
  	sum(pa.amount) as revenue
from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_payments') }} as pa on o.order_id=pa.payment_id
join {{ ref('stg_order_items') }} as oi on o.order_id=oi.order_id
join {{ ref('stg_products') }} as pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
group by year, month, category 
order by year, month






















