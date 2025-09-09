
  
    

  create  table "northwind"."analytics"."order_count_year+month_diff__dbt_tmp"
  
  
    as
  
  (
    select 
    date_part('year', ord.order_date) as year,
    date_part('month', ord.order_date) as month,
    count(ord.order_id) as count_order,
    max(count(ord.order_id)) over (partition by date_part('year', ord.order_date)) as max_in_year,
    count(ord.order_id) - max(count(ord.order_id)) over (partition by date_part('year', ord.order_date)) as diff_from_max
from "northwind"."analytics"."stg_order_count_year" as ord
group by year, month
order by year, month
  );
  