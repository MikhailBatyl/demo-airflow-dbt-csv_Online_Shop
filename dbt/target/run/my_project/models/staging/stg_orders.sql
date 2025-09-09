
  create view "northwind"."analytics"."stg_orders__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."orders"
  );