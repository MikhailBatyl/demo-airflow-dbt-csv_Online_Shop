
  create view "northwind"."analytics"."stg_order_details__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."order_items"
  );