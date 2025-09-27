WITH payment_stats AS (
    SELECT 
        p.payment_method,
        DATE_TRUNC('month', o.order_date)::date as month,
        SUM(p.amount) as total_revenue,
        COUNT(DISTINCT o.order_id) as paid_orders,
        ROUND(SUM(p.amount) * 1.0 / COUNT(DISTINCT o.order_id), 2) as avg_check
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
),
monthly_totals AS (
    SELECT 
        month,
        SUM(total_revenue) as total_monthly_revenue
    FROM payment_stats
    GROUP BY month
)
SELECT 
    ps.month,
    ps.payment_method,
    ps.paid_orders,
    round(ps.total_revenue, 2)::numeric,
    ps.avg_check,
    ROUND((ps.total_revenue * 100.0 / mt.total_monthly_revenue)::numeric, 2) as revenue_share_percent
FROM payment_stats as ps
JOIN monthly_totals as mt ON ps.month = mt.month
ORDER BY ps.month, ps.total_revenue desc
