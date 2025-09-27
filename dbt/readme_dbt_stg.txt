from {{ ref('stg_order_count_year') }} as o
join {{ ref('stg_customers') }} as cu on o.customer_id = cu.customer_id
join {{ ref('stg_payments') }} as pa on o.order_id=pa.order_id 


{{ ref('stg_order_items') }}
{{ ref('stg_products') }}
{{ ref('stg_reviews') }} 