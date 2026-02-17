with products_new as (
	select
		o.*,
		pa.*,
		oi.*,
		pr.*
	from {{ ref('stg_order_count_year') }} as o
	join {{ ref('stg_payments') }} as pa on o.order_id = pa.order_id
	join {{ ref('stg_order_items') }} as oi on o.order_id = oi.order_id
	join {{ ref('stg_products') }} as pr on oi.product_id = pr.product_id
	where pa.transaction_status = 'Completed'
),
orders_new as (
	select
		date_part('year', pn.order_date) as year,
	    date_part('month', pn.order_date) as month,
	    sum(pn.amount) as revenue
	from products_new as pn
	group by year, month
	order by year, month, revenue
)
select 
	*
from orders_new
