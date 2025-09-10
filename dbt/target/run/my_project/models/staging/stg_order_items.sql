
  create view "northwind"."analytics"."stg_order_items__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."order_items"
  );