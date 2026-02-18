WITH sku_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(oi.quantity) as total_quantity
    FROM {{ ref('stg_order_count_year') }} as o
    JOIN {{ ref('stg_order_items') }} as oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_products') }} as p ON oi.product_id = p.product_id
    JOIN {{ ref('stg_payments') }} as pay ON o.order_id = pay.order_id
    WHERE pay.transaction_status = 'Completed'
    GROUP BY p.product_id, p.product_name
),
ranked AS (
    SELECT
        product_id,
        product_name,
        total_quantity,
        RANK() OVER (ORDER BY total_quantity DESC) as rnk
    FROM sku_sales
)
SELECT
    CASE 
        WHEN rnk <= 10 THEN product_name
        ELSE 'Other'
    END AS product_group,
    SUM(total_quantity) as total_quantity
FROM ranked
GROUP BY product_group
ORDER BY total_quantity DESC
