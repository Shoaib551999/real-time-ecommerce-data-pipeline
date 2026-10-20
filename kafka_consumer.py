
import json
from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "ecommerce-python-consumer",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["ecommerce-orders"])

print("Waiting for customer orders...")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {message.error()}")
            continue

        try:
            order = json.loads(
                message.value().decode("utf-8")
            )

            print("\nNew Order Received!")
            print(f"Order ID: {order['order_id']}")
            print(f"Product: {order['product_name']}")
            print(f"Quantity: {order['quantity']}")
            print(f"Total Amount: ${order['total_amount']:.2f}")
            print(f"Status: {order['order_status']}")

        except (json.JSONDecodeError, KeyError, TypeError) as error:
            print(f"Invalid order message: {error}")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
