WITH customer_forder AS (
    SELECT
       	o.customer_id,
       	min(o.order_date) as first_order_date
    FROM {{ ref('stg_order_count_year') }} as o
    group by o.customer_id
),
orders_labeled as (
	select 
		o.order_id,
		o.customer_id,
		o.order_date,
		case
			when DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', cf.first_order_date)
				then 'New'
			else 'Returning'
		end as customer_type
	from {{ ref('stg_order_count_year') }} as o
	join customer_forder as cf on o.customer_id=cf.customer_id
)
SELECT 
	DATE_TRUNC('month', o.order_date)::date as month,
    customer_type,
    COUNT(DISTINCT o.customer_id) as customers_count,
    COUNT(DISTINCT o.order_id) as orders_count
FROM orders_labeled as o
GROUP BY DATE_TRUNC('month', o.order_date)::date, customer_type
ORDER BY month, customer_type


