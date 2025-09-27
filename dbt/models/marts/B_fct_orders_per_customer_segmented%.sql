WITH first_order AS (
    SELECT                                       -- Шаг 1: Находим дату первой покупки каждого клиента
        customer_id,
        MIN(order_date) as first_order_date
    FROM {{ ref('stg_order_count_year') }}
    GROUP BY customer_id
),
orders_with_cohort AS (
    SELECT                                        -- Шаг 2: Присваиваем каждому заказу клиента его когорту
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date as order_month,
        DATE_TRUNC('month', f.first_order_date)::date as cohort_month
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN first_order as f 
      ON o.customer_id = f.customer_id
),
orders_labeled AS (
    SELECT                                          -- Шаг 3: Считаем, сколько месяцев прошло с первой покупки
        customer_id,
        cohort_month,
        order_month,
        (EXTRACT(YEAR FROM order_month) - EXTRACT(YEAR FROM cohort_month)) * 12 + (EXTRACT(MONTH FROM order_month) - EXTRACT(MONTH FROM cohort_month)) as months_since_first
    FROM orders_with_cohort
),
cohort_sizes AS (
    SELECT                                         -- Шаг 4: Размер каждой когорты (кол-во клиентов с первой покупкой)
        cohort_month,
        COUNT(DISTINCT customer_id) as cohort_size
    FROM orders_labeled
    WHERE months_since_first = 0
    GROUP BY cohort_month
),
retention_counts AS (
    SELECT                                         -- Шаг 5: Считаем, сколько клиентов вернулись в каждый последующий месяц
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_id) as retained_customers
    FROM orders_labeled
    GROUP BY cohort_month, months_since_first
)
SELECT                                             -- Шаг 6: Рассчитываем процент удержания
    r.cohort_month,
    r.months_since_first,
    r.retained_customers,
    cs.cohort_size,
    ROUND(100.0 * r.retained_customers / cs.cohort_size, 2) as retention_rate
FROM retention_counts as r
JOIN cohort_sizes as cs 
  ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_first

