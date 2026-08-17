from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="fineguard.silver.customer",
    comment="Parshed and cleansed customer data"
)
@dp.expect_or_drop("Valid customer id","customer_id IS NOT NULL")
def silver_customer():
    bronzeDf = spark.readStream.table("fineguard.bronze.customers")
    transformedDf = bronzeDf.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),  # sikki: Properly converting string '2025-09-30' to Date, like a boss!
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit"),
        F.col("update_timestamp"),
        F.current_timestamp().alias("silver_ingestion")  # Change 2: New timestamp column
    )
    return transformedDf

