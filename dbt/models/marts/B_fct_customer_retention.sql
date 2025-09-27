WITH customer_orders AS (
    select                                                    -- Шаг 1: Собираем все покупки клиентов по месяцам
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date as month
    FROM {{ ref('stg_order_count_year') }} as o						    												
    JOIN {{ ref('stg_payments') }} as p                           
        ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)
),
cohorts AS (                                                 -- Шаг 2: Определяем когорту каждого клиента (месяц первой покупки)
    SELECT
        customer_id,
        MIN(month) as cohort_month
    FROM customer_orders
    GROUP BY customer_id
),
orders_with_cohort AS (                                     -- Шаг 3: Соединяем покупки клиентов с их когортой
    SELECT
        co.customer_id,
        c.cohort_month,
        co.month,
        DATE_PART('month', AGE(co.month, c.cohort_month)) as months_since_cohort
    FROM customer_orders as co
    JOIN cohorts as c ON co.customer_id = c.customer_id
),
cohort_sizes AS (                                         -- Шаг 4: Считаем размер каждой когорты (сколько клиентов сделали первую покупку)
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM cohorts
    GROUP BY cohort_month
),
retention AS (                                           -- Шаг 5: Считаем, сколько клиентов из когорты делали заказы в последующие месяцы
    SELECT
        cohort_month,
        months_since_cohort,
        COUNT(DISTINCT customer_id) as retained_customers
    FROM orders_with_cohort
    GROUP BY cohort_month, months_since_cohort
)                                                       -- Шаг 6: Рассчитываем retention % = доля удержанных клиентов относительно размера когорты
SELECT
    r.cohort_month,
    r.months_since_cohort,
    r.retained_customers,
    cs.cohort_size,
    ROUND(r.retained_customers::numeric / cs.cohort_size, 3) as retention_rate
FROM retention as r
JOIN cohort_sizes as cs ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_cohort

