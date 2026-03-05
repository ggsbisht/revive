create streaming table customers_bronze
as 
select *,_metadata.file_path as file_name,current_timestamp() as load_time 
from cloud_files('/Volumes/workspace/landing/landing/customers/','csv',map('cloudFiles.inferColumnTypes','True'));

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

