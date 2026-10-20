

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Create a local Spark session
spark = (
    SparkSession.builder
    .appName("EcommerceSparkBasics")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Sample e-commerce orders
orders = [
    ("ORD-1001", "Laptop", 2, 750.0),
    ("ORD-1002", "Phone", 1, 500.0),
    ("ORD-1003", "Headphones", 3, 80.0),
    ("ORD-1004", "Laptop", 1, 750.0)
]

# Create a DataFrame
df = spark.createDataFrame(
    orders,
    ["order_id", "product", "quantity", "unit_price"]
)

print("Original orders:")
df.show()

# Calculate the total value of each order
processed_df = df.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)

print("Processed orders:")
processed_df.show()

# Calculate sales by product
sales_df = (
    processed_df
    .groupBy("product")
    .agg(spark_sum("total_amount").alias("total_sales"))
)

print("Sales by product:")
sales_df.show()

spark.stop()
