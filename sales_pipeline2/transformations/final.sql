create materialized view city_wise_sales_gold
as 
select city,sum(total_amount) as total_sales
from live.orders_silver o
join live.customers_silver c
on o.id=c.customer_id
group by city