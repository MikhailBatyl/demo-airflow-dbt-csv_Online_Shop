
  
    

  create  table "northwind"."analytics"."AOV_chek__dbt_tmp"
  
  
    as
  
  (
    select 
    aov.product_name,	
	aov.category,
	round(avg(aov.price)::numeric, 2) avg_price,
	max(aov.price) max_price,
	min(aov.price) min_price
from "northwind"."analytics"."stg_AOV" as aov
group by aov.product_name, aov.category
  );
  