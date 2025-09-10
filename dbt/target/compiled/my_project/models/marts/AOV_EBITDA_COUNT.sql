with calc_ebitda as (
	select
		round((sum(pr.price * oi.quantity) / count(distinct o.order_id))::numeric, 2) as order_value_avg
	from "northwind"."analytics"."stg_orders" as o
	join "northwind"."analytics"."stg_payments" as pa on o.order_id=pa.payment_id
	join "northwind"."analytics"."stg_order_details" as oi on o.order_id=oi.order_id
	join "northwind"."analytics"."stg_products" as pr on oi.product_id=pr.product_id
	where pa.transaction_status='Completed'	
)


select
	order_value_avg
from calc_ebitda
order by order_value_avg