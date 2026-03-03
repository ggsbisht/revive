create streaming table orders_bronze 
comment "ingesting orders data"
TBLPROPERTIES("quality"="bronze")
as
select *, _metadata.file_path as file_name,current_timestamp() as load_time 
from cloud_files('/Volumes/workspace/landing/landing/orders/','csv',map('cloudFiles.inferColumnTypes','True'));


create streaming table customers_bronze
as 
select *,_metadata.file_path as file_name,current_timestamp() as load_time 
from cloud_files('/Volumes/workspace/landing/landing/customers/','csv',map('cloudFiles.inferColumnTypes','True'));

create streaming table orders_silver_cleaned
( constraint valid_orders expect(id is not null)
  on violation drop row
) as
select orderid as id,orderdate as order_date,totalamount as total_amount,status,file_name,load_time
from stream(orders_bronze) ;

create streaming table customer_silver_cleaned(
  constraint valid_customer expect(customer_id is not null)
  on violation drop row
)
as
select customerid as customer_id,customername as customer_name,address as city,dateofbirth as dob,registrationdate as customer_since,load_time
from stream(customers_bronze);

create streaming table customers_silver;
create flow customers_silver_flow as auto cdc into customers_silver
from stream(customer_silver_cleaned)
keys(customer_id)
sequence by load_time
stored as scd type 2;


create streaming table orders_silver;
create flow orders_silver_flow as auto cdc into orders_silver
from stream(orders_silver_cleaned)
keys(id)
sequence by load_time;

create materialized view city_wise_sales_gold
as 
select city,sum(total_amount) as total_sales
from live.orders_silver o
join live.customers_silver c
on o.id=c.customer_id
group by city