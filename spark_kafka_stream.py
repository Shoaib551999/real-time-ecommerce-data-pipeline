
import os
import logging

from db_writer import save_orders
from data_quality import get_valid_order_condition
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    lit,
    to_json,
    struct,
    abs as spark_abs,
    round as spark_round
)
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
# Capture malformed JSON parsing errors
CORRUPT_RECORD_FIELD = "_corrupt_record"
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
    StructField("_corrupt_record", StringType(), True),
])

# Convert Kafka bytes into JSON strings
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS order_json"
)

# Parse JSON messages
orders_df = (
    json_df
    .select(
        col("order_json"),
        from_json(
            col("order_json"),
            order_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record"
            }
        ).alias("order")
    )
    .select(
        "order_json",
        "order.*"
    )
)

# Identify malformed JSON messages
malformed_orders = (
    orders_df
    .filter(
        col("_corrupt_record").isNotNull()
    )
    .select(
        col("order_json"),
        lit("MALFORMED_JSON").alias("rejection_reason")
    )
)

# Shared validation rules for confirmed orders

valid_order_condition = get_valid_order_condition()

# Identify invalid orders for the Dead Letter Queue
invalid_orders = orders_df.filter(
    col("_corrupt_record").isNull() &
    (
        col("order_status").isNull() |
        (~col("order_status").isin(
            "PLACED", "CONFIRMED", "CANCELLED"
        )) |
        (
            (col("order_status") == "CONFIRMED") &
            (~valid_order_condition.eqNullSafe(True))
        )
    )
)

# Prepare rejected orders with a reason
dlq_orders = invalid_orders.select(
    col("order_id"),
    col("order_json"),
    col("order_status"),
    lit("INVALID_ORDER_DATA").alias("rejection_reason")
)

# Format rejected orders for Kafka DLQ
dlq_kafka_df = dlq_orders.select(
    to_json(
        struct(
            col("order_id"),
            col("order_json"),
            col("order_status"),
            col("rejection_reason")
        )
    ).alias("value")
)

# Format malformed JSON messages for Kafka DLQ
malformed_kafka_df = malformed_orders.select(
    to_json(
        struct(
            col("order_json"),
            col("rejection_reason")
        )
    ).alias("value")
)

# Combine rejected messages into one DLQ DataFrame
all_dlq_df = dlq_kafka_df.unionByName(
    malformed_kafka_df.select(
        "value"
    )
)

# Keep only valid confirmed orders for PostgreSQL
valid_orders = (
    orders_df
    .filter(valid_order_condition)
    .drop("order_json", "_corrupt_record")
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

# Configuration for rejected Kafka events
KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "ecommerce-orders-dlq"
)


# Start Kafka Dead Letter Queue streaming query
dlq_query = (
    all_dlq_df.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("topic", KAFKA_DLQ_TOPIC)
    .option(
        "checkpointLocation",
        "checkpoints/kafka_dlq"
    )
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

logger.info("Kafka Dead Letter Queue streaming started")

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
