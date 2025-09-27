select *
from {{ source('northwind', 'reviews') }}