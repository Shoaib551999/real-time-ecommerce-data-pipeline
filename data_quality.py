
from pyspark.sql.functions import (
    col,
    abs as spark_abs,
    round as spark_round
)


def get_valid_order_condition():
    """Return the validation condition for confirmed orders."""

    return (
        col("_corrupt_record").isNull() &
        (col("order_status") == "CONFIRMED") &
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("product_name").isNotNull() &
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("total_amount") >= 0) &
        (
            spark_abs(
                spark_round(
                    col("quantity") * col("unit_price"), 2
                ) - col("total_amount")
            ) <= 0.001
        )
    )
