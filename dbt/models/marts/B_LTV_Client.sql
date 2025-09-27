WITH customer_first_order AS (
    SELECT
        o.customer_id,
        MIN(o.order_date) as first_order_date
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p 
        ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id
),
orders_with_cohort AS (
    SELECT
        o.customer_id,
        o.order_date,
        p.amount,
        DATE_TRUNC('month', cfo.first_order_date)::date as cohort_month,
        DATE_PART('month', AGE(DATE_TRUNC('month', o.order_date), DATE_TRUNC('month', cfo.first_order_date))) as months_since_cohort
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_payments') }} as p 
        ON o.order_id = p.order_id
    JOIN customer_first_order as cfo
        ON o.customer_id = cfo.customer_id
    WHERE p.transaction_status = 'Completed'
),
cohort_ltv AS (
    SELECT
        cohort_month,
        months_since_cohort,
        AVG(SUM(amount)) OVER (PARTITION BY cohort_month, customer_id ORDER BY months_since_cohort ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_ltv_per_customer
    FROM orders_with_cohort
    GROUP BY cohort_month, customer_id, months_since_cohort
)
SELECT
    cohort_month,
    months_since_cohort,
    AVG(cumulative_ltv_per_customer) as avg_ltv
FROM cohort_ltv
GROUP BY cohort_month, months_since_cohort
ORDER BY cohort_month, months_since_cohort


