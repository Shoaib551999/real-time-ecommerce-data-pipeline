
import os
import logging

from db_writer import save_orders
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ecommerce-orders"
)

# Create Spark session
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

logger.info("Spark started successfully")

# Read streaming messages from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Define incoming order schema
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
    StructField("event_timestamp", StringType(), True),
])

# Convert Kafka bytes into JSON strings
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS order_json"
)

# Parse JSON messages
orders_df = (
    json_df
    .select(
        from_json(
            col("order_json"),
            order_schema
        ).alias("order")
    )
    .select("order.*")
)

# Filter valid confirmed orders
valid_orders = orders_df.filter(
    (col("order_status") == "CONFIRMED") &
    (col("product_name").isNotNull()) &
    (col("quantity") > 0) &
    (col("total_amount") >= 0)
)

# Save micro-batches to PostgreSQL
def write_orders_to_postgres(batch_df, batch_id):
    batch_size = 100
    orders = []
    processed_count = 0

    try:
        for row in batch_df.toLocalIterator():
            orders.append(row.asDict())

            if len(orders) >= batch_size:
                save_orders(orders)
                processed_count += len(orders)
                orders.clear()

        if orders:
            save_orders(orders)
            processed_count += len(orders)

        logger.info(
            "Batch %s: processed %s orders for PostgreSQL",
            batch_id,
            processed_count
        )

    except Exception:
        logger.exception(
            "Batch %s: failed to process orders",
            batch_id
        )
        raise


# Start PostgreSQL streaming query
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

logger.info("PostgreSQL streaming pipeline started")

# Keep Spark streaming
spark.streams.awaitAnyTermination()
