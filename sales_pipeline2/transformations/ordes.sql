create streaming table orders_bronze 
comment "ingesting orders data"
TBLPROPERTIES("quality"="bronze")
as
select *, _metadata.file_path as file_name,current_timestamp() as load_time 
from cloud_files('/Volumes/workspace/landing/landing/orders/','csv',map('cloudFiles.inferColumnTypes','True'));

create streaming table orders_silver_cleaned
( constraint valid_orders expect(id is not null)
  on violation drop row
) as
select orderid as id,orderdate as order_date,totalamount as total_amount,status,file_name,load_time
from stream(orders_bronze) ;

create streaming table orders_silver;
create flow orders_silver_flow as auto cdc into orders_silver
from stream(orders_silver_cleaned)
keys(id)
sequence by load_time;

