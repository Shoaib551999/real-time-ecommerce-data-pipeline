
from data_quality import get_valid_order_condition
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import abs as spark_abs, round as spark_round
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def test_invalid_order_quantity():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DataQualityTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        # Create sample orders without Kafka
        data = [
            ("ORD-001", 2),
            ("ORD-002", 0),
            ("ORD-003", -3),
        ]

        df = spark.createDataFrame(
            data,
            ["order_id", "quantity"]
        )

        invalid_orders = df.filter(
            col("quantity") <= 0
        )

        results = [
            row["order_id"]
            for row in invalid_orders.collect()
        ]

        assert results == ["ORD-002", "ORD-003"]

    finally:
        spark.stop()



def test_missing_order_quantity():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("MissingQuantityTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        data = [
            ("ORD-001", 2),
            ("ORD-002", None),
            ("ORD-003", 0),
        ]

        df = spark.createDataFrame(
            data,
            ["order_id", "quantity"]
        )

        invalid_orders = df.filter(
            col("quantity").isNull() |
            (col("quantity") <= 0)
        )

        results = [
            row["order_id"]
            for row in invalid_orders.collect()
        ]

        assert results == ["ORD-002", "ORD-003"]

    finally:
        spark.stop()


def test_incorrect_order_total():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("IncorrectTotalTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        data = [
            ("ORD-001", 2, 500.0, 1000.0),
            ("ORD-002", 3, 200.0, 500.0),
            ("ORD-003", 1, 100.0, 150.0),
        ]

        df = spark.createDataFrame(
            data,
            ["order_id", "quantity", "unit_price", "total_amount"]
        )

        invalid_orders = df.filter(
            spark_abs(
                spark_round(
                    col("quantity") * col("unit_price"), 2
                ) - col("total_amount")
            ) > 0.001
        )

        results = [
            row["order_id"]
            for row in invalid_orders.collect()
        ]

        assert results == ["ORD-002", "ORD-003"]

    finally:
        spark.stop()



def test_malformed_json_detection():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("MalformedJSONTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])

        data = [
            ('{"order_id":"ORD-001"}',),
            ('{"order_id":',),
        ]

        df = spark.createDataFrame(data, ["order_json"])

        parsed_df = df.select(
            "order_json",
            from_json(
                col("order_json"),
                schema,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record"
                }
            ).alias("order")
        ).select("order_json", "order.*")

        malformed = parsed_df.filter(
            col("_corrupt_record").isNotNull()
        )

        results = [
            row["order_json"]
            for row in malformed.collect()
        ]

        assert results == ['{"order_id":']

    finally:
        spark.stop()



def test_shared_validation_condition():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("SharedValidationTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        data = [
            ("ORD-001", "CUST-001", "PROD-101", "Laptop",
             "CONFIRMED", 2, 500.0, 1000.0, None),

            ("ORD-002", "CUST-002", "PROD-101", "Laptop",
             "CONFIRMED", 0, 500.0, 0.0, None),

            ("ORD-003", "CUST-003", "PROD-101", "Laptop",
             "CONFIRMED", 2, 500.0, 900.0, None),

            ("ORD-004", "CUST-004", "PROD-101", "Laptop",
             "CANCELLED", 2, 500.0, 1000.0, None),
        ]

        schema = """
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            product_name STRING,
            order_status STRING,
            quantity INT,
            unit_price DOUBLE,
            total_amount DOUBLE,
            _corrupt_record STRING
        """

        df = spark.createDataFrame(data, schema=schema)

        valid_orders = df.filter(
            get_valid_order_condition()
        )

        results = [
            row["order_id"]
            for row in valid_orders.collect()
        ]

        assert results == ["ORD-001"]

    finally:
        spark.stop()



def test_null_safe_invalid_order():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("NullSafeValidationTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        schema = """
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            product_name STRING,
            order_status STRING,
            quantity INT,
            unit_price DOUBLE,
            total_amount DOUBLE,
            _corrupt_record STRING
        """

        data = [
            ("ORD-001", "CUST-001", "PROD-101",
             "Laptop", "CONFIRMED", 2, 500.0, 1000.0, None),

            ("ORD-002", None, "PROD-101",
             "Laptop", "CONFIRMED", 2, 500.0, 1000.0, None),
        ]

        df = spark.createDataFrame(data, schema=schema)

        invalid_orders = df.filter(
            (col("order_status") == "CONFIRMED") &
            (~get_valid_order_condition().eqNullSafe(True))
        )

        results = [
            row["order_id"]
            for row in invalid_orders.collect()
        ]

        assert results == ["ORD-002"]

    finally:
        spark.stop()
