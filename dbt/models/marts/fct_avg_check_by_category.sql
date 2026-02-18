WITH avg_check_per_category AS (
    SELECT 
        p.category,
        DATE_TRUNC('month', o.order_date)::date as month,
        ROUND(SUM(oi.quantity * oi.price_at_purchase)::numeric * 1.0 / COUNT(DISTINCT o.order_id), 2) as avg_check
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_order_items') }} as oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_products') }} as p ON oi.product_id = p.product_id
    JOIN {{ ref('stg_payments') }} as pay ON o.order_id = pay.order_id
    WHERE pay.transaction_status = 'Completed'
    GROUP BY p.category, DATE_TRUNC('month', o.order_date)::date
)
SELECT * FROM avg_check_per_category
