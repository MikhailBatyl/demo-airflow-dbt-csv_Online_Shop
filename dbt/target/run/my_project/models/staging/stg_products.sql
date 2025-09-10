
  create view "northwind"."analytics"."stg_products__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."products"
  );