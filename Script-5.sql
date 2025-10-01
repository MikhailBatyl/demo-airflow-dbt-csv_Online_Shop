
--TRUNCATE TABLE order_items, payment, orders, shipments, customers, products, suppliers RESTART IDENTITY CASCADE;


/*
 БЛОК А [Продажи и конверсия]

 Кол-во заказов по годам ------ ✅  (dbt = order_count_year.sql) +
*/

select 
	date_part('year', order_date) as year,	
	count(order_id) as count_order
from orders
group by year
order by year ASC;


--Динамика по месяцу + max значение по заказам и расчет дельта отклонений по месяцам от max ---------  ✅ (dbt = order_count_year+month_diff) +

select
    date_part('year', order_date) as year,
    date_part('month', order_date) as month,
    count(order_id) as count_order,
    max(count(order_id)) over (partition by date_part('year', order_date)) as max_in_year,
    count(order_id) - max(count(order_id)) over (partition by date_part('year', order_date)) as diff_from_max
from orders
group by year, month
order by year, month;



/*
 Задание 2 ------

 Cредний чек по категориям и продуктам - Средний чек (AOV) ✅ ------ (dbt = AOV_EBITDA_COUNT.sql) +

 AOV = Общая выручка / Количество заказов
*/

select
	round((sum(pr.price * oi.quantity) / count(distinct o.order_id))::numeric, 2) avr_order_value
from orders o
join payment pa on o.order_id=pa.payment_id
join order_items oi on o.order_id=oi.order_id
join products pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
order by avr_order_value;


--- Средний чек по Категориям ----------------- (dbt = AOV_category.sql) +
select
	pr.category category,
	round((sum(pr.price * oi.quantity) / count(distinct o.order_id))::numeric, 2) avr_order_value
from orders o
join payment pa on o.order_id=pa.payment_id
join order_items oi on o.order_id=oi.order_id
join products pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
group by pr.category
order by avr_order_value;


--- Средний чек по Продукту ------------------- (dbt = AOV_product_name.sql) +
select
	pr.product_name product_name,
	round((sum(pr.price * oi.quantity) / count(distinct o.order_id))::numeric, 2) avr_order_value
from orders o
join payment pa on o.order_id=pa.payment_id
join order_items oi on o.order_id=oi.order_id
join products pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
group by pr.product_name
order by avr_order_value;


/*Задание 3 ------

 Выручка (Revenue)  ✅ ------

 Revenue = сумма всех оплаченных заказов 
 Это ключевой показатель продаж. Главное — учитывать только завершённые/оплаченные заказы, чтобы не завышать результат.

*/

--выручка по годам и месяцу (dbt = EBITDA_year+month.sql) +

with products_new as (
	select
		o.*,
		pa.*,
		oi.*,
		pr.*
	from orders o
	join payment pa on o.order_id=pa.payment_id
	join order_items oi on o.order_id=oi.order_id
	join products pr on oi.product_id=pr.product_id
	where pa.transaction_status='Completed'
),
orders_new as (
	select
		date_part('year', pn.order_date) as year,
	    date_part('month', pn.order_date) as month,
	    sum(pn.amount) revenue
	from products_new pn
	group by year, month
	order by year, month, revenue
)
select 
	*
from orders_new;


--выручка по годам + кол-во заказов + max_amount -- (dbt = EBITDA_year+count_orders+max_amount.sql) +

select
	date_part('year', o.order_date) as year,
    sum(pa.amount) as revenue,
    count(o.order_id) as order_id,
    max(pa.amount) as max_amount
from orders as o
join payment as pa on o.order_id=pa.payment_id
join order_items as oi on o.order_id=oi.order_id
join products as pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
group by year
order by year, revenue;

-- Еще рассмотреть динамику по времени -- (dbt = EBITDA_diff_time.sql) +

SELECT 
    date_part('year', o.order_date) as year,
	date_part('month', o.order_date) as month,
    SUM(p.amount) as revenue,
    LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)) as prev_revenue,
    (SUM(p.amount) - LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date))) AS diff_absolute,
    ROUND(
        100.0 * (SUM(p.amount) - LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)))
        / NULLIF(LAG(SUM(p.amount)) OVER (ORDER BY DATE_PART('year', o.order_date), DATE_PART('month', o.order_date)),0)
    ) as diff_percent
FROM orders as o
JOIN payment as p ON o.order_id = p.order_id
WHERE p.transaction_status='Completed'
GROUP BY date_part('month', o.order_date), date_part('year', o.order_date)
ORDER BY year, month;

-- Выручка по категориям товаров -- (dbt = EBITDE_category.sql) +

select
	date_part('year', o.order_date) as year,
	date_part('month', o.order_date) as month,
  	pr.category,
  	sum(pa.amount) as revenue
from orders as o
join payment as pa on o.order_id=pa.payment_id
join order_items as oi on o.order_id=oi.order_id
join products as pr on oi.product_id=pr.product_id
where pa.transaction_status='Completed'
group by year, month, category 
order by year, month;


/*
 Задание 4 ------

  4. Количество товаров на заказ (Items per Order)  ------

 📌 Определение

 Items per Order (IPO) показывает, сколько товаров в среднем покупатель кладёт в один заказ.

 👉 Формула:

 IPO = Общее количество товаров/Количество заказов

 Нам важно связать заказ и его товарные позиции.
*/
	
	
-- Количество товаров на заказ (Items per Order)-- (dbt = Count_Items_per_Order.sql) +

select 
	round(avg(items_per_order)) as avg_items_per_order
from (select 
			o.order_id,
			sum(oi.quantity) items_per_order
	 from orders o
	 join order_items oi on o.order_id = oi.order_id
	 join payment p on o.order_id=p.order_id
	 where p.transaction_status = 'Completed'
	 group by o.order_id) as tq
	 
-- Динамика по месяцам --(dbt = Dunam_Items_month.sql) +

select 
	date_part('month', o.order_date) as month,	
	round(avg(items_per_order)) as avg_items_per_order
from (select 
			o.order_id,
			sum(oi.quantity) items_per_order
	 from orders o
	 join order_items oi on o.order_id = oi.order_id
	 join payment p on o.order_id=p.order_id
	 where p.transaction_status = 'Completed'
	 group by o.order_id) as tq	
join orders o on tq.order_id = o.order_id
group by month
order by month;
	 	 
-- Динамика по категориям товаров-- (dbt = Dunam_items_category.sql)

select 
	pr.category,
	round(avg(items_per_order)) as avg_items_per_order
from (select 
			o.order_id, 
			sum(oi.quantity) items_per_order
	 from orders o
	 join order_items oi on o.order_id = oi.order_id
	 join payment p on o.order_id=p.order_id
	 where p.transaction_status = 'Completed'
	 group by o.order_id) as tq	
join orders o on tq.order_id = o.order_id
join products pr on tq.order_id=pr.product_id
group by pr.category 
order by avg_items_per_order;

-------------------------------------------------


/*
 [Блок B: Клиенты]
 ────────────────────────────────────────────────────────────
 | Метрики:                                                  |
 | 1. Новые vs. повторные покупатели ✅                      |
 | 2. Частота покупок (Orders per Customer) ✅              |
 | 3. LTV (приблизительно) ✅                               |
 | 4. Retention (повторные заказы) ✅                        |
 |                                                          |
 | Визуализация:                                            |
 | - Круговые диаграммы: доля новых/повторных клиентов    |
 | - Линейные графики: повторные заказы по неделям        |
 | - Таблицы: топ-10 клиентов по выручке                  |
 ────────────────────────────────────────────────────────────

--------------------------------------------------

 | Метрики:      
                                            |
 | 1. Новые vs. повторные покупатели ✅            (dbt = B_fct_customer_types.sql) +

🔹 Суть метрики

 Новый покупатель (New Customer) — сделал первый заказ в компании.

 Повторный покупатель (Returning Customer) — сделал заказ, но у него уже были заказы ранее.

 Эта метрика показывает:

 Как бизнес привлекает новых клиентов.

 Насколько эффективно работает удержание (retention) через повторные покупки.

 🔹 Как считать

 Для каждого клиента (customer_id) находим дату первого заказа.

 В заказах смотрим:

 Если order_date = min(order_date) → заказ от нового покупателя.

 Если order_date > min(order_date) → заказ от повторного покупателя.

 Считаем агрегаты по месяцам/годам.
*/	 


WITH customer_forder AS (
    SELECT
       	o.customer_id,
       	min(o.order_date) as first_order_date
    FROM orders o
    group by o.customer_id
),
orders_labeled as (
	select 
		o.order_id,
		o.customer_id,
		o.order_date,
		case
			when DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', cf.first_order_date)
				then 'New'
			else 'Returning'
		end as customer_type
	from orders o
	join customer_forder cf on o.customer_id=cf.customer_id
)
SELECT 
	DATE_TRUNC('month', o.order_date)::date AS month,
    customer_type,
    COUNT(DISTINCT o.customer_id) AS customers_count,
    COUNT(DISTINCT o.order_id) AS orders_count
FROM orders_labeled o
GROUP BY DATE_TRUNC('month', o.order_date)::date, customer_type
ORDER BY month, customer_type;

-------------------------------------------------------------------------------

--🔹 2. Частота покупок (Orders per Customer)
-- 📌 Определение

-- Orders per Customer (OPC) показывает, сколько заказов в среднем делает один клиент за определённый период.

-- Высокая частота → клиенты активно возвращаются.

-- Низкая частота → может быть проблема с удержанием.

-- 📊 Данные в датасете

-- orders → order_id, customer_id, order_date, status.

-- payment → опционально для фильтра успешных заказов (transaction_status='Completed').

-- 🛠 Формула
-- OPC=Количество всех заказов/Количество уникальных клиентов

-- 1️⃣ Средняя частота покупок за всё время	​

select 
	count(distinct o.order_id)::float/count(distinct cu.customer_id) as order_per_customer
from orders o
join customers cu on o.customer_id = cu.customer_id
join payment pa on o.order_id=pa.order_id 
where pa.transaction_status = 'Completed';

-------------------------------------------------------------------------------------

-- 2️⃣ Динамика по месяцам

SELECT
    DATE_TRUNC('month', o.order_date)::date AS month,
    COUNT(DISTINCT o.order_id)::float / COUNT(DISTINCT o.customer_id) AS orders_per_customer
from orders o
join customers cu on o.customer_id = cu.customer_id
join payment pa on o.order_id=pa.order_id 
where pa.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month;

-------------------------------------------------------------------------------------

--3️⃣ Частота по сегменту клиентов (New vs Returning) (dbt = B_fct_orders_per_customer_segmented.sql)

--Если есть витрина orders_labeled (New / Returning):

WITH customer_forder AS (
    SELECT
       	o.customer_id,
       	min(o.order_date) as first_order_date
    FROM orders o
    group by o.customer_id
),
orders_labeled as (
	select 
		o.order_id,
		o.customer_id,
		o.order_date,
		case
			when DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', cf.first_order_date)
				then 'New'
			else 'Returning'
		end as customer_type
	from orders o
	join customer_forder cf on o.customer_id=cf.customer_id
)
SELECT 
	DATE_TRUNC('month', o.order_date)::date AS month,
    customer_type,
    COUNT(DISTINCT order_id)::float / COUNT(DISTINCT customer_id) AS orders_per_customer
FROM orders_labeled o
GROUP BY DATE_TRUNC('month', o.order_date)::date, customer_type
ORDER BY month, customer_type;

-- Код dbt-модели
-- models/fct_orders_per_customer.sql

--📂 Структура модели

--Модель будет называться fct_orders_per_customer_segmented.sql.
--Она будет строиться на основе:

--orders

--order_items (чтобы связать клиента с заказом)

--products (для корректного первого заказа)

-- 🛠 Код dbt-модели
-- models/fct_orders_per_customer_segmented.sql

WITH customer_first_order AS (
    SELECT
        o.customer_id,
        MIN(o.order_date) AS first_order_date
    FROM orders o
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
        END AS customer_type
    FROM orders o
    JOIN customer_first_order cfo 
        ON o.customer_id = cfo.customer_id
    WHERE o.status = 'completed'
),
monthly_stats AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS month,
        customer_type,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM orders_labeled
    GROUP BY 1, 2
)
SELECT
    month,
    customer_type,
    total_orders,
    total_customers,
    (total_orders::float / NULLIF(total_customers, 0)) AS orders_per_customer
FROM monthly_stats
ORDER BY month, customer_type;

---------------------------------------------------------------------------------------------

-- 🔹 3. LTV (Lifetime Value, приближенно)
-- 📌 Определение

-- LTV = суммарная прибыль/выручка, которую приносит клиент за всё время взаимодействия.

-- В e-commerce чаще считают упрощённый вариант:

-- LTV=ARPU×AOV×CLV

-- но без маржи и маркетинговых затрат мы можем использовать Revenue per Customer (RPC):

-- LTV≈Общая выручка/Количество уникальных клиентов
	​
-- 📊 Данные для расчёта из датасета

-- orders → order_id, customer_id, order_date, status

-- order_items → product_id, quantity

-- products → price

-- payment → amount, transaction_status

-- 👉 Для LTV нам достаточно связки: customer_id → его заказы → revenue.
	

	
	--1️⃣ LTV по каждому клиенту	
SELECT
    o.customer_id,
    SUM(p.amount) AS customer_revenue
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY o.customer_id;

	-- 2️⃣ Средний LTV по всей базе
SELECT
    SUM(p.amount)::float / COUNT(DISTINCT o.customer_id) AS avg_ltv
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed';

	--🔹 3️⃣ Динамика LTV по месяцам

SELECT
    DATE_TRUNC('month', o.order_date)::date AS month,
    round((SUM(p.amount)::float / COUNT(DISTINCT o.customer_id))::numeric, 2) AS avg_ltv
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY 1
ORDER BY month;
-- 👉 Показывает, как меняется средний LTV клиентов, сделавших заказы в конкретный месяц.


-- 📂 Модель: fct_customer_ltv_cohort.sql

-- Эта витрина позволит ответить на вопрос:
-- 👉 «Как растёт средний LTV клиентов, привлечённых в определённый месяц, через 1, 2, 3 … N месяцев?»

-- 🛠 Код dbt-модели
-- models/fct_customer_ltv_cohort.sql

WITH customer_first_order AS (
    SELECT
        o.customer_id,
        MIN(o.order_date) AS first_order_date
    FROM {{ ref('orders') }} o
    JOIN {{ ref('payment') }} p 
        ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id
),
orders_with_cohort AS (
    SELECT
        o.customer_id,
        o.order_date,
        p.amount,
        DATE_TRUNC('month', cfo.first_order_date)::date AS cohort_month,
        DATE_PART('month', AGE(DATE_TRUNC('month', o.order_date), DATE_TRUNC('month', cfo.first_order_date))) AS months_since_cohort
    FROM {{ ref('orders') }} o
    JOIN {{ ref('payment') }} p 
        ON o.order_id = p.order_id
    JOIN customer_first_order cfo
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
    AVG(cumulative_ltv_per_customer) AS avg_ltv
FROM cohort_ltv
GROUP BY cohort_month, months_since_cohort
ORDER BY cohort_month, months_since_cohort;

-------------------------------------------------------------------------------------

-- 🔹 4. Retention (повторные заказы)
-- 📌 Определение

-- Retention показывает, какая доля клиентов возвращается и делает повторные заказы спустя определённое время (месяц, квартал).

-- В e-commerce retention измеряют:

-- Repeat Purchase Rate (RPR) = % клиентов, которые сделали более 1 заказа.

-- Monthly Retention = доля клиентов, вернувшихся в последующие месяцы после первой покупки.

-- Cohort Retention = как удерживаются клиенты, пришедшие в определённый месяц.

-- 📊 Данные в датасете Kaggle

-- orders → order_id, customer_id, order_date, status.

-- payment → фильтруем только Completed.

-- Этого достаточно, чтобы считать retention.


SELECT
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END)::float
    / COUNT(DISTINCT customer_id) AS repeat_purchase_rate
FROM (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id
) tq;

 -- 2️⃣ Monthly Retention (доля вернувшихся клиентов)

WITH customer_orders AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date AS month
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)
),
cohort AS (
    SELECT
        customer_id,
        MIN(month) AS cohort_month
    FROM customer_orders
    GROUP BY customer_id
)
SELECT
    c.cohort_month,
    co.month,
    COUNT(DISTINCT co.customer_id) AS retained_customers
FROM customer_orders co
JOIN cohort c ON co.customer_id = c.customer_id
GROUP BY c.cohort_month, co.month
ORDER BY c.cohort_month, co.month;


-- Модель для DBT

-- 📂 Модель: fct_customer_retention.sql

-- Эта модель рассчитает:

-- когортный месяц (месяц первой покупки клиента),

-- месяц повторной активности,

-- количество удержанных клиентов,

-- % retention относительно размера когорты.

-- 🛠 Код dbt-модели с пояснениями
-- models/fct_customer_retention.sql

WITH customer_orders AS (
    select                                                    -- Шаг 1: Собираем все покупки клиентов по месяцам
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date AS month
    FROM orders o						    -- В dbt = FROM {{ref('orders')}} o												
    JOIN payment p                          -- В dbt = JOIN {{ref('payment')}} p 
        ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)
),
cohorts AS (                                                 -- Шаг 2: Определяем когорту каждого клиента (месяц первой покупки)
    SELECT
        customer_id,
        MIN(month) AS cohort_month
    FROM customer_orders
    GROUP BY customer_id
),
orders_with_cohort AS (                                     -- Шаг 3: Соединяем покупки клиентов с их когортой
    SELECT
        co.customer_id,
        c.cohort_month,
        co.month,
        DATE_PART('month', AGE(co.month, c.cohort_month)) AS months_since_cohort
    FROM customer_orders co
    JOIN cohorts c ON co.customer_id = c.customer_id
),
cohort_sizes AS (                                         -- Шаг 4: Считаем размер каждой когорты (сколько клиентов сделали первую покупку)
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohorts
    GROUP BY cohort_month
),
retention AS (                                           -- Шаг 5: Считаем, сколько клиентов из когорты делали заказы в последующие месяцы
    SELECT
        cohort_month,
        months_since_cohort,
        COUNT(DISTINCT customer_id) AS retained_customers
    FROM orders_with_cohort
    GROUP BY cohort_month, months_since_cohort
)                                                       -- Шаг 6: Рассчитываем retention % = доля удержанных клиентов относительно размера когорты
SELECT
    r.cohort_month,
    r.months_since_cohort,
    r.retained_customers,
    cs.cohort_size,
    ROUND(r.retained_customers::numeric / cs.cohort_size, 3) AS retention_rate
FROM retention r
JOIN cohort_sizes cs ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_cohort;

-------------------------------------------------------------------------------------------------

-- вариант №2 
--Запрос, который сразу считает retention в процентах (кохортный анализ по месяцам). Его можно вставить в Metabase и строить heatmap:
-- dbt модели


-- models/fct_customer_retention.sql

WITH first_order AS (
    -- Шаг 1: Находим дату первой покупки каждого клиента
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM {{ ref('orders') }}
    GROUP BY customer_id
),
orders_with_cohort AS (
    -- Шаг 2: Присваиваем каждому заказу клиента его когорту
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date AS order_month,
        DATE_TRUNC('month', f.first_order_date)::date AS cohort_month
    FROM {{ ref('orders') }} o
    JOIN first_order f 
      ON o.customer_id = f.customer_id
),
orders_labeled AS (
    -- Шаг 3: Считаем, сколько месяцев прошло с первой покупки
    SELECT
        customer_id,
        cohort_month,
        order_month,
        (EXTRACT(YEAR FROM order_month) - EXTRACT(YEAR FROM cohort_month)) * 12 +
        (EXTRACT(MONTH FROM order_month) - EXTRACT(MONTH FROM cohort_month)) AS months_since_first
    FROM orders_with_cohort
),
cohort_sizes AS (
    -- Шаг 4: Размер каждой когорты (кол-во клиентов с первой покупкой)
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM orders_labeled
    WHERE months_since_first = 0
    GROUP BY cohort_month
),
retention_counts AS (
    -- Шаг 5: Считаем, сколько клиентов вернулись в каждый последующий месяц
    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_id) AS retained_customers
    FROM orders_labeled
    GROUP BY cohort_month, months_since_first
)
-- Шаг 6: Рассчитываем процент удержания
SELECT
    r.cohort_month,
    r.months_since_first,
    r.retained_customers,
    cs.cohort_size,
    ROUND(100.0 * r.retained_customers / cs.cohort_size, 2) AS retention_rate
FROM retention_counts r
JOIN cohort_sizes cs 
  ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_first;

-------------------------------------------------------------------------------------------

-- Следующий блок:

-- [Блок C: Продукты и категории]
-- ────────────────────────────────────────────────────────────
-- | Метрики:                                                  |
-- | 1. Выручка по категориям ✅                               |
-- | 2. Количество заказанных товаров по SKU ✅                |
-- | 3. Средний чек по категории ✅                             |
-- |                                                          |
-- | Визуализация:                                            |
-- | - Столбчатые диаграммы: топ-10 категорий по выручке     |
-- | - Таблицы: топ-10 товаров по продажам  
--------------------------------------------------------------------------------------------

--  1. Выручка по категориям ✅

select 
	pr.category,
	DATE_TRUNC('month', o.order_date)::date as month,
	round(sum(oi.quantity * oi.price_at_purchase)) as revenue
from orders o
join order_items oi on o.order_id=oi.order_id 
join products pr on oi.product_id=pr.product_id
join payment pa on o.order_id=pa.payment_id
where pa.transaction_status ='Completed'
group by pr.category, DATE_TRUNC('month', o.order_date)::date
ORDER BY month, revenue DESC;

------- Для dbt----------------------------------

WITH revenue_per_category AS (
    SELECT 
        pr.category,
        DATE_TRUNC('month', o.order_date)::date AS month,
        ROUND(SUM(oi.quantity * oi.price_at_purchase) * 1.0 / COUNT(DISTINCT o.order_id), 2) as avg_check
    FROM {{ ref('orders') }} o
    JOIN {{ ref('order_items') }} oi on o.order_id=oi.order_id 
    JOIN {{ ref('products') }} pr on oi.product_id=pr.product_id
    JOIN {{ ref('payment') }} pa on o.order_id=pa.payment_id
    WHERE pay.transaction_status = 'Completed'
    group by pr.category, DATE_TRUNC('month', o.order_date)::date
)
SELECT * 
FROM revenue_per_category
ORDER BY month, revenue DESC;

--⚡ Так мы получим витрину с выручкой по категориям, готовую для визуализации.

----------------------------------------

--2. Количество заказанных товаров по SKU ✅

--Эта метрика показывает:

--сколько единиц каждого товара (SKU) было заказано за период,

--позволяет выявить хиты продаж и товары с низким спросом.

--Используется для:

--управления запасами,

--планирования закупок,

--оценки эффективности ассортимента.


-- Items_per_SKU=∑(order_items.quantity)

-- с группировкой по product_id (SKU). (dbt = C_Items_per_SKU.sql)


SELECT 
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS total_quantity,
    DATE_TRUNC('month', o.order_date)::date AS month
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN payment pay ON o.order_id = pay.order_id
WHERE pay.transaction_status = 'Completed'
GROUP BY p.product_id, p.product_name, DATE_TRUNC('month', o.order_date)
ORDER BY total_quantity DESC;

---------------------------------- для dbt------------------------------

WITH sku_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.quantity) AS total_quantity
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN payment pay ON o.order_id = pay.order_id
    WHERE pay.transaction_status = 'Completed'
    GROUP BY p.product_id, p.product_name
),
ranked AS (
    SELECT
        product_id,
        product_name,
        total_quantity,
        RANK() OVER (ORDER BY total_quantity DESC) AS rnk
    FROM sku_sales
)
SELECT
    CASE 
        WHEN rnk <= 10 THEN product_name
        ELSE 'Other'
    END AS product_group,
    SUM(total_quantity) AS total_quantity
FROM ranked
GROUP BY product_group
ORDER BY total_quantity DESC;

--------------------------------------------------------------------------------

-- | 3. Средний чек по категории ✅  (dbt = C_avg_check_per_category.sql)

SELECT 
    p.category,
    DATE_TRUNC('month', o.order_date)::date AS month,
    ROUND((SUM(oi.quantity * oi.price_at_purchase) * 1.0 / COUNT(DISTINCT o.order_id))::numeric, 2) AS avg_check
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN payment pay ON o.order_id = pay.order_id
WHERE pay.transaction_status = 'Completed'
GROUP BY p.category, DATE_TRUNC('month', o.order_date)
ORDER BY month, avg_check DESC;

--- для dbt -------------------------------------

WITH avg_check_per_category AS (
    SELECT 
        p.category,
        DATE_TRUNC('month', o.order_date)::date AS month,
        ROUND(SUM(oi.quantity * oi.unit_price) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS avg_check
    FROM {{ ref('orders') }} o
    JOIN {{ ref('order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('products') }} p ON oi.product_id = p.product_id
    JOIN {{ ref('payment') }} pay ON o.order_id = pay.order_id
    WHERE pay.transaction_status = 'Completed'
    GROUP BY p.category, DATE_TRUNC('month', o.order_date)
)
SELECT * FROM avg_check_per_category;

--------------------------------------------------------------------

-- [Блок D: Платежи]
-- ────────────────────────────────────────────────────────────
-- | Метрики:                                                  |
-- | 1. Выручка по способам оплаты ✅                          |
-- | 2. Количество оплаченных заказов ✅                        |
-- | 3. Средний чек по способу оплаты ✅                        |
-- |                                                          |
-- | Визуализация:                                            |
-- | - Круговые диаграммы: распределение выручки по методам  |
-- | - Линейные графики: динамика выручки по дням            |




-- | Метрики:                                                  |
-- | 1. Выручка по способам оплаты ✅    (dbt = D_revenue_by_payment%.sql)

-- 🔹 Что это такое

-- Эта метрика показывает, сколько выручки приносит каждый способ оплаты (например, карта, PayPal, наложенный платеж, Apple Pay).

-- Используется для:

-- анализа популярности платёжных методов,

-- оптимизации комиссий и расходов,

-- принятия решений о добавлении/удалении способов оплаты.

SELECT 
    p.payment_method,
    DATE_TRUNC('month', o.order_date)::date AS month,
    round(SUM(p.amount)::numeric, 1) AS revenue
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
ORDER BY month, revenue DESC;

-------------- Для  dbt ----------------------------------------

WITH revenue_by_payment AS (
    SELECT 
        p.payment_method,
        DATE_TRUNC('month', o.order_date)::date AS month,
        SUM(p.amount) AS revenue
    FROM {{ ref('orders') }} o
    JOIN {{ ref('payment') }} p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
)
SELECT * FROM revenue_by_payment;

---- или ------------------------------------------

--- Добавим к метрике Выручка по способам оплаты ещё один слой — доля (%) от общей выручки.

--🔹 SQL-запрос с долями (%)
WITH revenue_by_payment AS (
    SELECT 
        p.payment_method,
        DATE_TRUNC('month', o.order_date)::date AS month,
        SUM(p.amount) AS revenue
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
),
total_revenue AS (
    SELECT 
        month,
        SUM(revenue) AS total_revenue
    FROM revenue_by_payment
    GROUP BY month
)
SELECT 
    rbp.month,
    rbp.payment_method,
    rbp.revenue,
    ROUND(rbp.revenue * 100.0 / tr.total_revenue, 2) AS revenue_share_percent
FROM revenue_by_payment rbp
JOIN total_revenue tr ON rbp.month = tr.month
ORDER BY rbp.month, rbp.revenue DESC;

----------------------------------------------------------------------

-- | 2. Количество оплаченных заказов ✅        (dbt = D_total_orders.sql)

-- 🔹 Смысл метрики

-- Показывает количество заказов, где платеж прошёл успешно (например, Completed).

-- Отличается от общей метрики «Количество заказов», т.к. может быть корзина или заказ без оплаты.

-- Используется для:

-- оценки реальной конверсии в оплату,

-- сравнения разных методов оплаты,

-- анализа потерь (брошенные корзины, отмены).

-- 🔹 SQL-запрос (по месяцам)

SELECT 
    DATE_TRUNC('month', o.order_date)::date AS month,
    p.payment_method,
    COUNT(DISTINCT o.order_id) AS paid_orders
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.payment_method
ORDER BY month, paid_orders DESC;

--------Для dbt -----------------------------------------

--Количество оплаченных заказов вывести коэффициент конверсии оплаты (Payment Conversion Rate, PCR).

--🔹 Смысл метрики Payment Conversion Rate (PCR) показывает, какая доля заказов была успешно оплачена.
-- Формула:

-- 𝑃𝐶𝑅= (Количество оплаченных заказов/Общее количество заказов)×100%

WITH total_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date AS month,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM orders o
    GROUP BY DATE_TRUNC('month', o.order_date)
),
paid_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date AS month,
        COUNT(DISTINCT o.order_id) AS paid_orders
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT 
    t.month,
    t.total_orders,
    p.paid_orders,
    ROUND(CAST(p.paid_orders AS NUMERIC) / NULLIF(t.total_orders, 0) * 100, 2) AS payment_conversion_rate
FROM total_orders t
LEFT JOIN paid_orders p ON t.month = p.month
ORDER BY t.month;

---------------------------------------------------------------------------------

-- 3. Средний чек по способу оплаты ✅ (dbt = D_payment_stats.sql)
-- 🔹 Что это такое

-- Средний чек по способу оплаты (Average Order Value by Payment Method) — это средняя сумма оплаченного заказа для каждого метода оплаты.

-- Используется для:

-- анализа покупательской способности по разным платёжным каналам,

-- выявления методов с высоким/низким средним чеком,

-- оценки влияния способа оплаты на доходность.

SELECT 
    p.payment_method,
    DATE_TRUNC('month', o.order_date)::date AS month,
    ROUND(SUM(p.amount) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS avg_check
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
ORDER BY month, avg_check DESC;

--------или----------------------

----- Для dbt--------------------------
-- средний чек по способу оплаты + доля метода оплаты в общей выручке.

WITH payment_stats AS (
    SELECT 
        p.payment_method,
        DATE_TRUNC('month', o.order_date)::date AS month,
        SUM(p.amount) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS paid_orders,
        ROUND(SUM(p.amount) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS avg_check
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY p.payment_method, DATE_TRUNC('month', o.order_date)
),
monthly_totals AS (
    SELECT 
        month,
        SUM(total_revenue) AS total_monthly_revenue
    FROM payment_stats
    GROUP BY month
)
SELECT 
    ps.month,
    ps.payment_method,
    ps.paid_orders,
    round(ps.total_revenue, 2)::numeric,
    ps.avg_check,
    ROUND((ps.total_revenue * 100.0 / mt.total_monthly_revenue)::numeric, 2) AS revenue_share_percent
FROM payment_stats ps
JOIN monthly_totals mt ON ps.month = mt.month
ORDER BY ps.month, ps.total_revenue desc;

------------------------------------------------------------------------------------------------------

-- | 2. Количество оплаченных заказов ✅ |----------  (dbt = D_Payment_Conversion_Rate.sql)

SELECT 
    DATE_TRUNC('month', o.order_date)::date AS month,
    COUNT(DISTINCT o.order_id) AS paid_orders
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month;

---🔹 Расширенный вариант (по методам оплаты)

SELECT 
    DATE_TRUNC('month', o.order_date)::date AS month,
    p.payment_method,
    COUNT(DISTINCT o.order_id) AS paid_orders
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.payment_method
ORDER BY month, paid_orders DESC;

--------------------------------------------------------------

--Делаем связку: Количество оплаченных заказов + Коэффициент оплаты (Payment Conversion Rate, PCR).

--🔹 Напоминание

--Количество оплаченных заказов (Paid Orders) → абсолютное число заказов со статусом Completed.

--Payment Conversion Rate (PCR) → доля оплаченных заказов от всех созданных.

WITH total_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date AS month,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM orders o
    GROUP BY DATE_TRUNC('month', o.order_date)
),
paid_orders AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::date AS month,
        COUNT(DISTINCT o.order_id) AS paid_orders
    FROM orders o
    JOIN payment p ON o.order_id = p.order_id
    WHERE p.transaction_status = 'Completed'
    GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT 
    t.month,
    t.total_orders,
    p.paid_orders,
    ROUND(p.paid_orders * 100.0 / NULLIF(t.total_orders,0), 2) AS payment_conversion_rate
FROM total_orders t
LEFT JOIN paid_orders p ON t.month = p.month
ORDER BY t.month;

------------------------------------------------------------------------------------------------------

-- | 3. Средний чек по способу оплаты ✅     (dbt = D_AOV_payment_method.sql)

--Средний чек по способу оплаты показывает:
--👉 Какова средняя сумма оплаченного заказа в зависимости от того, каким методом клиент платил (например, Credit Card, PayPal, Bank Transfer).

--Это помогает понять:

--какие платёжные каналы приносят более крупные заказы,

--где клиенты склонны тратить больше/меньше,

--как распределяется выручка по методам оплаты.

SELECT 
    DATE_TRUNC('month', o.order_date)::date AS month,
    p.payment_method,
    COUNT(DISTINCT o.order_id) AS paid_orders,
    SUM(p.amount) AS total_revenue,
    ROUND(SUM(p.amount) * 1.0 / NULLIF(COUNT(DISTINCT o.order_id),0), 2) AS avg_check
FROM orders o
JOIN payment p ON o.order_id = p.order_id
WHERE p.transaction_status = 'Completed'
GROUP BY DATE_TRUNC('month', o.order_date), p.payment_method
ORDER BY month, total_revenue DESC;

------------------------------------------------------------------------------------------------------------

--[Блок E: Отзывы]                   (dbt = E_Reviews + positive+negative.sql)

--────────────────────────────────────────────────────────────
--| Метрики:                                                  |
--| 1. Средний рейтинг товара ✅                               |
--| 2. Количество отзывов по товару ✅                        |
--| 3. Соотношение положительных/отрицательных отзывов ✅    |
--|                                                          |
--| Визуализация:                                            |
--| - Таблицы: топ-10 товаров по рейтингу                   |
--| - Столбчатые диаграммы: количество отзывов по категориям|
--────────────────────────────────────────────────────────────




-- Метрика показывает среднее значение оценок покупателей по каждому товару.

-- Используется, чтобы понять качество продукта с точки зрения клиентов.

-- Помогает выявлять товары с высоким риском возвратов или низким спросом из-за плохих оценок.

-- Может быть использована в витрине для сортировки/рекомендаций.

	​

WITH base_reviews AS (
    SELECT
        r.review_id,
        r.product_id,
        r.rating::numeric AS rating,  -- приводим к numeric
        r.review_date::date AS review_date,
        DATE_TRUNC('month', r.review_date::date)::date AS month
    FROM reviews r
),
product_stats AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category AS category_name,
        COUNT(br.review_id) AS reviews_count,
        AVG(br.rating) AS avg_rating,
        SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN br.rating <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(br.review_id),0),2) AS positive_share,
        ROUND(SUM(CASE WHEN br.rating <= 2 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(br.review_id),0),2) AS negative_share,
        CASE
            WHEN SUM(CASE WHEN br.rating <= 2 THEN 1 ELSE 0 END) = 0 THEN NULL
            ELSE ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric 
                       / SUM(CASE WHEN br.rating <= 2 THEN 1 ELSE 0 END),2)
        END AS pos_neg_ratio
    FROM products p
    LEFT JOIN base_reviews br ON p.product_id = br.product_id
    GROUP BY p.product_id, p.product_name, p.category
),
global_stats AS (
    SELECT
        AVG(rating) AS global_avg_rating,
        50 AS m
    FROM base_reviews
),
weighted_ratings AS (
    SELECT
        ps.*,
        ROUND(((ps.reviews_count::float / (ps.reviews_count + gs.m)) * ps.avg_rating +(gs.m::float / (ps.reviews_count + gs.m)) * gs.global_avg_rating::numeric)::numeric, 2) AS weighted_rating
    FROM product_stats ps
    CROSS JOIN global_stats gs
),
monthly_reviews AS (
    SELECT
        br.month,
        br.product_id,
        COUNT(br.review_id) AS monthly_reviews_count,
        ROUND(AVG(br.rating),2) AS monthly_avg_rating
    FROM base_reviews br
    GROUP BY br.month, br.product_id
),
monthly_with_window AS (
    SELECT
        mr.*,
        SUM(mr.monthly_reviews_count) OVER (PARTITION BY mr.product_id ORDER BY mr.month) AS cumulative_reviews,
        mr.monthly_avg_rating - LAG(mr.monthly_avg_rating) OVER (PARTITION BY mr.product_id ORDER BY mr.month) AS delta_avg_rating,
        RANK() OVER (PARTITION BY mr.month ORDER BY mr.monthly_reviews_count DESC) AS product_rank_by_reviews
    FROM monthly_reviews mr
)
SELECT
    wr.product_id,
    wr.product_name,
    wr.category_name,
    wr.reviews_count,
    ROUND(wr.avg_rating,2) AS avg_rating,
    wr.weighted_rating,
    wr.positive_reviews,
    wr.negative_reviews,
    wr.positive_share,
    wr.negative_share,
    wr.pos_neg_ratio,
    mw.month,
    mw.monthly_reviews_count,
    mw.monthly_avg_rating,
    mw.cumulative_reviews,
    mw.delta_avg_rating,
    mw.product_rank_by_reviews
FROM weighted_ratings wr
LEFT JOIN monthly_with_window mw ON wr.product_id = mw.product_id
ORDER BY mw.month DESC NULLS LAST, mw.product_rank_by_reviews;



---------------------------------------------------------------------------------------------

-----для DBT---------------------

-- 🛠 dbt-модель с оконными функциями: reviews_dashboard.sql

-- Что нового добавилось через оконные функции

-- cumulative_reviews → общее накопленное количество отзывов по товару.

-- delta_avg_rating → разница среднего рейтинга по сравнению с прошлым месяцем.

-- product_rank_by_reviews → место товара в рейтинге по числу отзывов в конкретном месяце.

WITH base_reviews AS (
    SELECT
        r.review_id,
        r.product_id,
        r.rating,
        r.review_date::date AS review_date,
        DATE_TRUNC('month', r.review_date)::date AS month
    FROM {{ ref('reviews') }} r
),
product_stats AS (
    SELECT
        p.product_id,
        p.product_name,
        COUNT(r.review_id) AS reviews_count,
        AVG(r.rating) AS avg_rating
    FROM {{ ref('products') }} p
    LEFT JOIN base_reviews r ON p.product_id = r.product_id
    GROUP BY p.product_id, p.product_name
),
global_stats AS (
    SELECT
        AVG(r.rating) AS global_avg_rating,
        50 AS m  -- минимальное число отзывов для надёжного рейтинга
    FROM base_reviews r
),
weighted_ratings AS (
    SELECT
        ps.product_id,
        ps.product_name,
        ps.reviews_count,
        ps.avg_rating,
        ROUND((
            (ps.reviews_count::float / (ps.reviews_count + gs.m)) * ps.avg_rating +
            (gs.m::float / (ps.reviews_count + gs.m)) * gs.global_avg_rating
        )::numeric, 2) AS weighted_rating
    FROM product_stats ps
    CROSS JOIN global_stats gs
),
monthly_reviews AS (
    SELECT
        br.month,
        br.product_id,
        COUNT(br.review_id) AS monthly_reviews_count,
        ROUND(AVG(br.rating), 2) AS monthly_avg_rating
    FROM base_reviews br
    GROUP BY br.month, br.product_id
),
monthly_with_window AS (
    SELECT
        mr.*,
        -- Кумулятивное количество отзывов по продукту
        SUM(mr.monthly_reviews_count) OVER (
            PARTITION BY mr.product_id ORDER BY mr.month
        ) AS cumulative_reviews,
        
        -- Отклонение среднего рейтинга от прошлого месяца
        mr.monthly_avg_rating - LAG(mr.monthly_avg_rating) OVER (
            PARTITION BY mr.product_id ORDER BY mr.month
        ) AS delta_avg_rating,
        
        -- Ранк товаров по числу отзывов в месяце
        RANK() OVER (
            PARTITION BY mr.month ORDER BY mr.monthly_reviews_count DESC
        ) AS product_rank_by_reviews
    FROM monthly_reviews mr
),
category_reviews AS (
    SELECT
        c.category_id,
        c.category_name,
        COUNT(r.review_id) AS reviews_count,
        ROUND(AVG(r.rating), 2) AS avg_rating
    FROM {{ ref('categories') }} c
    JOIN {{ ref('products') }} p ON c.category_id = p.category_id
    LEFT JOIN base_reviews r ON p.product_id = r.product_id
    GROUP BY c.category_id, c.category_name
)
SELECT
    wr.product_id,
    wr.product_name,
    wr.reviews_count,
    ROUND(wr.avg_rating, 2) AS avg_rating,
    wr.weighted_rating,
    mw.month,
    mw.monthly_reviews_count,
    mw.monthly_avg_rating,
    mw.cumulative_reviews,
    mw.delta_avg_rating,
    mw.product_rank_by_reviews,
    cr.category_name,
    cr.reviews_count AS category_reviews_count,
    cr.avg_rating AS category_avg_rating
FROM weighted_ratings wr
LEFT JOIN monthly_with_window mw ON wr.product_id = mw.product_id
LEFT JOIN {{ ref('products') }} p ON wr.product_id = p.product_id
LEFT JOIN category_reviews cr ON p.category_id = cr.category_id
ORDER BY mw.month DESC, mw.product_rank_by_reviews;

----------------------------------------------------------------------------

--  3. Соотношение положительных/отрицательных отзывов ✅   


-- Новые метрики:

-- positive_reviews — количество положительных отзывов.

-- negative_reviews — количество отрицательных отзывов.

-- positive_share — доля положительных отзывов.

-- negative_share — доля отрицательных отзывов.

-- pos_neg_ratio — соотношение положительных к отрицательным.


WITH base_reviews AS (
    SELECT
        r.review_id,
        r.product_id,
        r.rating::numeric AS rating,
        r.review_date::date AS review_date,
        DATE_TRUNC('month', r.review_date::date)::date AS month
    FROM reviews r
),
product_stats AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category AS category_name,
        COUNT(br.review_id) AS reviews_count,
        AVG(br.rating) AS avg_rating,
        SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(br.review_id),0), 2) AS positive_share,
        ROUND(SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END)::numeric/NULLIF(COUNT(br.review_id),0), 2) AS negative_share,
        CASE WHEN SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END) = 0 THEN NULL ELSE ROUND(SUM(CASE WHEN br.rating >= 4 THEN 1 ELSE 0 END)::numeric / SUM(CASE WHEN br.rating <= 3 THEN 1 ELSE 0 END), 2)
        END AS pos_neg_ratio
    FROM products p
    LEFT JOIN base_reviews br ON p.product_id = br.product_id
    GROUP BY p.product_id, p.product_name, p.category
),
global_stats AS (
    SELECT
        AVG(rating) AS global_avg_rating,
        1623 AS m  -- минимальное число отзывов для надежного рейтинга
    FROM base_reviews
),
weighted_ratings AS (
    SELECT
        ps.*,
        ROUND(((ps.reviews_count::float / (ps.reviews_count + gs.m)) * ps.avg_rating::numeric + (gs.m::float / (ps.reviews_count + gs.m)) * gs.global_avg_rating::numeric)::numeric, 2) AS weighted_rating
    FROM product_stats ps
    CROSS JOIN global_stats gs
),
monthly_reviews AS (
    SELECT
        br.month,
        br.product_id,
        COUNT(br.review_id) AS monthly_reviews_count,
        ROUND(AVG(br.rating), 2) AS monthly_avg_rating
    FROM base_reviews br
    GROUP BY br.month, br.product_id
),
monthly_with_window AS (
    SELECT
        mr.*,
        SUM(mr.monthly_reviews_count) OVER (PARTITION BY mr.product_id ORDER BY mr.month) AS cumulative_reviews,
        mr.monthly_avg_rating - LAG(mr.monthly_avg_rating) OVER (PARTITION BY mr.product_id ORDER BY mr.month) AS delta_avg_rating,
        RANK() OVER (PARTITION BY mr.month ORDER BY mr.monthly_reviews_count DESC) AS product_rank_by_reviews
    FROM monthly_reviews mr
)
SELECT
    wr.product_id,
    wr.product_name,
    wr.category_name,
    wr.reviews_count,
    ROUND(wr.avg_rating,2) AS avg_rating,
    wr.weighted_rating,
    wr.positive_reviews,
    wr.negative_reviews,
    wr.positive_share,
    wr.negative_share,
    wr.pos_neg_ratio,
    mw.month,
    mw.monthly_reviews_count,
    mw.monthly_avg_rating,
    mw.cumulative_reviews,
    mw.delta_avg_rating,
    mw.product_rank_by_reviews
FROM weighted_ratings wr
LEFT JOIN monthly_with_window mw ON wr.product_id = mw.product_id
ORDER BY mw.month DESC NULLS LAST, mw.product_rank_by_reviews;



-------------------------------------------------------------------------------------------------------------



