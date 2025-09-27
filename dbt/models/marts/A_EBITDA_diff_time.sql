SELECT 
    date_part('year', o.order_date) as year,
	date_part('month', o.order_date) as month,
    SUM(p.amount) as revenue,
    LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)) as prev_revenue,
    (SUM(p.amount) - LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date))) as diff_absolute,
    ROUND(
        100.0 * (SUM(p.amount) - LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)))
        / NULLIF(LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)),0)
    ) as diff_percent
from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_payments') }} as p on o.order_id=p.payment_id
WHERE p.transaction_status='Completed'
GROUP BY date_part('month', o.order_date), date_part('year', o.order_date)
ORDER BY year, month



