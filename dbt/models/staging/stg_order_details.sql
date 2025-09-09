select *
from {{ source('northwind', 'order_items') }}