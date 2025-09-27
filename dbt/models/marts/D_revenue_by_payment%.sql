WITH revenue_by_payment AS (
    SELECT 
        p.payment_method,
        DATE_TRUNC('month', o.order_date)::date as month,
        SUM(p.amount) as revenue
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
),
total_revenue AS (
    SELECT 
        month,
        SUM(revenue) as total_revenue
    FROM revenue_by_payment
    GROUP BY month
)
SELECT 
    rbp.month,
    rbp.payment_method,
    rbp.revenue,
    ROUND(rbp.revenue * 100.0 / tr.total_revenue, 2) as revenue_share_percent
FROM revenue_by_payment as rbp
JOIN total_revenue as tr ON rbp.month = tr.month
ORDER BY rbp.month, rbp.revenue DESC


