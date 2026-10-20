
import json
import time
from confluent_kafka import Producer

from advanced_order_generator import (
    generate_order,
    validate_order
)

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Delivered to {message.topic()} "
            f"at offset {message.offset()}"
        )

order_number = 910000

try:
    for _ in range(5):
        order_number += 1

        order = generate_order(order_number)
        order["order_status"] = "CONFIRMED"

        if validate_order(order):
            producer.produce(
                "ecommerce-orders",
                key=order["order_id"],
                value=json.dumps(order).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            print(f"Queued order: {order['order_id']}")

        time.sleep(2)

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
