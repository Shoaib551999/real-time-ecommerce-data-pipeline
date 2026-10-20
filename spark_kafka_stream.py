import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)
from db_writer import save_orders
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql import SparkSession

# Create Spark session with Kafka connector
spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark started successfully!")

# Read live messages from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-orders")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    
)

# Define the structure of incoming JSON orders
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])

# Convert Kafka message bytes into JSON strings
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS order_json"
)

# Parse JSON using the schema
orders_df = (
    json_df
    .select(from_json(col("order_json"), order_schema).alias("order"))
    .select("order.*")
)
# Keep only confirmed orders with valid sales values
valid_orders = orders_df.filter(
    (col("order_status") == "CONFIRMED") &
    (col("product_name").isNotNull()) &
    (col("quantity") > 0) &
    (col("total_amount") >= 0)
)

def write_orders_to_postgres(batch_df, batch_id):
    try:
        orders = [
            row.asDict()
            for row in batch_df.collect()
        ]

        if not orders:
            logger.info("Batch %s: no orders to process", batch_id)
            return

        save_orders(orders)

        logger.info(
            "Batch %s: processed %s orders for PostgreSQL",
            batch_id,
            len(orders)
        )

    except Exception:
        logger.exception(
            "Batch %s: failed to process orders",
            batch_id
        )
        raise

postgres_query = (
    valid_orders.writeStream
    .foreachBatch(write_orders_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/postgres_orders"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Calculating live sales by product...")
print("Saving confirmed orders to PostgreSQL...")

spark.streams.awaitAnyTermination()