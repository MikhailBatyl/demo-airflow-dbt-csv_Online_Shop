select 
    date_part('year', ord.order_date) as year,	
	count(ord.order_id)
from {{ ref('stg_order_count_year') }} as ord
group by year
order by year ASC
