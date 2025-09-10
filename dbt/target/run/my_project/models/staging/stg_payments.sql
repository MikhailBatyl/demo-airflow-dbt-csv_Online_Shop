
  create view "northwind"."analytics"."stg_payments__dbt_tmp"
    
    
  as (
    select *
from "northwind"."public"."payment"
  );