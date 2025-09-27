WITH customer_first_order AS (
    SELECT
        o.customer_id,
        MIN(o.order_date) as first_order_date
    FROM {{ ref('stg_order_count_year') }} o
    WHERE o.status = 'completed'
    GROUP BY o.customer_id
),
orders_labeled AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        CASE
            WHEN DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', cfo.first_order_date)
                THEN 'New'
            ELSE 'Returning'
        END as customer_type
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN customer_first_order as cfo 
        ON o.customer_id = cfo.customer_id
    WHERE o.status = 'completed'
),
monthly_stats AS (
    SELECT
        DATE_TRUNC('month', order_date)::date as month,
        customer_type,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as total_customers
    FROM orders_labeled
    GROUP BY 1, 2
)
SELECT
    month,
    customer_type,
    total_orders,
    total_customers,
    (total_orders::float / NULLIF(total_customers, 0)) as orders_per_customer
FROM monthly_stats
ORDER BY month, customer_type

