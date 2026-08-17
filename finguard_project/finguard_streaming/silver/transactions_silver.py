from pyspark import pipelines as dp
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StringType, DoubleType, BooleanType,StructField,StructType


@dp.table(
    name="fineguard.silver.transactions",
    comment="Refined Transaction data"
)
@dp.expect_or_drop("Valid transaction id","transaction_id is not null")
@dp.expect_or_drop("Valid customer id","customer_id is not null")
@dp.expect_or_drop("Valid card number","card_number is not null")
@dp.expect_or_drop("Valid merchant id","merchant_id is not null")
@dp.expect("Valid Amount","amount > 0")
def silver_transactions():
    bronze_df=spark.readStream.table("fineguard.bronze.transactions")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True)
    ])

    transformedDf = bronze_df.select(
        from_json(col("value"), schema).alias("data"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kakfa_timestamp"),
        col("ingested_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        "data.*",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kakfa_timestamp",
        "bronze_ingestion_timestamp"
    )
    return transformedDf
