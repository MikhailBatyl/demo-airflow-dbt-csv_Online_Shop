
  create view "northwind"."analytics"."stg_order_count_year__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."orders"
  );