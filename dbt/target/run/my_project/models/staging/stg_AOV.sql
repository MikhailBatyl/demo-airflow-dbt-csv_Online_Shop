
  create view "northwind"."analytics"."stg_AOV__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."products"
  );