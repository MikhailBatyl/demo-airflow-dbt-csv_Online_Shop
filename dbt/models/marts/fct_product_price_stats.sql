select 
    pr.product_name,	
	pr.category,
	round(avg(pr.price)::numeric, 2) as avg_price,
	max(pr.price) as max_price,
	min(pr.price) as min_price
from {{ ref('stg_products') }} as pr
group by pr.product_name, pr.category
