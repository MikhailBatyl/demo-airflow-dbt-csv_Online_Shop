WITH revenue_per_category AS (
    SELECT 
        p.category,
        DATE_TRUNC('month', o.order_date)::date as month,
        SUM(oi.quantity * oi.price_at_purchase) as revenue
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_order_items') }} oi on o.order_id=oi.order_id 
    JOIN {{ ref('stg_products') }} as p on oi.product_id=p.product_id
    JOIN {{ ref('stg_payments') }} as pa on o.order_id=pa.payment_id
    WHERE pa.transaction_status = 'Completed'
    group by p.category, DATE_TRUNC('month', o.order_date)::date
)
SELECT * 
FROM revenue_per_category
ORDER BY month, revenue DESC




