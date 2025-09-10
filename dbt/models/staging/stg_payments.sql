select *
from {{ source('northwind', 'payment') }}
