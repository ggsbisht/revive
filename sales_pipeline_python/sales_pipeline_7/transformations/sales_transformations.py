from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp,col,sum as _sum
from utilities import utils

source_path=spark.conf.get("source")

@dp.table()
def orders_bronze():
    df=(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.inferColumnTypes","true")
        .load(f"{source_path}orders/")
    )
    return ( df.withColumn("filename",col("_metadata.file_name"))
            .withColumn("load_time",current_timestamp())
    )

#bronze: ingest customers

@dp.table()
def customers_bronze():
    df=(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.inferColumnTypes","true")
        .load(f"{source_path}customers/")
    )
    return ( df.withColumn("filename",col("_metadata.file_path"))
            .withColumn("load_time",current_timestamp())
    )

#silver:cleansed orders with expectations

@dp.table()
@dp.expect_or_drop("valid_orders","order_id is not null")
@dp.expect_or_drop("valid_customer","customer_id is not null")
def orders_silver_cleaned():
    df=spark.readStream.table("orders_bronze")
    return(
        df.selectExpr("orderid as order_id",
                      "orderdate as order_date",
                      "customerid as customer_id",
                      "totalamount as total_amount",
                      "status",
                      "filename as file_name",
                      "load_time"
        ).withColumn("totalamount_in_usd",utils.int_to_usd(col("total_amount")))
    )

#silver:cleansed customer with expectations

@dp.table()
@dp.expect_or_drop("valid_customer","customer_id is not null")
def customers_silver_cleaned():
    df=spark.readStream.table("customers_bronze")
    return(
        df.selectExpr("customerid as customer_id",
                      "customername as customer_name",
                      "address as city",
                      "dateofbirth as dob",
                      "registrationdate as registration_date",
                      "filename as file_name",
                      "load_time"
              )
    )

#silver:scd2 for customers using AUTO CDC

dp.create_streaming_table("customer_silver")
dp.create_auto_cdc_flow(
    target="customer_silver",
    source="customers_silver_cleaned",
    keys=["customer_id"],
    sequence_by=col("load_time"),
    stored_as_scd_type=2
)

#silver: scd type1 for orders using auto cdc

dp.create_streaming_table("orders_silver")
dp.create_auto_cdc_flow(
    target="orders_silver",
    source="orders_silver_cleaned",
    keys=["order_id"],
    sequence_by=col("load_time")
)


#gold: city_wise sales materialized view

@dp.materialized_view()
def city_wise_sales_gold():
    orders=spark.read.table("orders_silver")
    customers=spark.read.table("customer_silver")
    return (
        orders.join(customers,"customer_id")
        .groupBy("city")
        .agg(_sum("total_amount").alias("total_sales"))
    )
