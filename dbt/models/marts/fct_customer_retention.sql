WITH customer_orders AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date as month
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p
        ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)
),
cohorts AS (
    SELECT
        customer_id,
        MIN(month) as cohort_month
    FROM customer_orders
    GROUP BY customer_id
),
orders_with_cohort AS (
    SELECT
        co.customer_id,
        c.cohort_month,
        co.month,
        DATE_PART('month', AGE(co.month, c.cohort_month)) as months_since_cohort
    FROM customer_orders as co
    JOIN cohorts as c ON co.customer_id = c.customer_id
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM cohorts
    GROUP BY cohort_month
),
retention AS (
    SELECT
        cohort_month,
        months_since_cohort,
        COUNT(DISTINCT customer_id) as retained_customers
    FROM orders_with_cohort
    GROUP BY cohort_month, months_since_cohort
)
SELECT
    r.cohort_month,
    r.months_since_cohort,
    r.retained_customers,
    cs.cohort_size,
    ROUND(r.retained_customers::numeric / cs.cohort_size, 3) as retention_rate
FROM retention as r
JOIN cohort_sizes as cs ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_cohort
