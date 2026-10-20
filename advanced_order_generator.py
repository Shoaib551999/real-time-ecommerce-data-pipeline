
import json
import random
import time
from datetime import datetime, timezone

# Product catalog
products = [
    {
        "product_id": "PROD-101",
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 750.00
    },
    {
        "product_id": "PROD-102",
        "product_name": "Phone",
        "category": "Electronics",
        "price": 500.00
    },
    {
        "product_id": "PROD-103",
        "product_name": "Headphones",
        "category": "Accessories",
        "price": 80.00
    },
    {
        "product_id": "PROD-104",
        "product_name": "Monitor",
        "category": "Electronics",
        "price": 250.00
    }
]

payment_methods = [
    "Credit Card",
    "Debit Card",
    "PayPal"
]

order_statuses = [
    "PLACED",
    "CONFIRMED",
    "CANCELLED"
]


# Function to generate one order
def generate_order(order_number):
    product = random.choice(products)
    quantity = random.randint(1, 5)

    order = {
        "order_id": f"ORD-{order_number}",
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(
            quantity * product["price"], 2
        ),
        "payment_method": random.choice(payment_methods),
        "order_status": random.choice(order_statuses),
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }

    return order


# Function to validate an order
def validate_order(order):
    required_fields = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "event_timestamp"
    ]

    for field in required_fields:
        if field not in order:
            return False

    if not isinstance(order["quantity"], int):
        return False

    if order["quantity"] <= 0:
        return False

    if order["unit_price"] <= 0:
        return False

    expected_total = round(
        order["quantity"] * order["unit_price"], 2
    )

    if abs(order["total_amount"] - expected_total) > 0.001:
        return False

    return True


# Save valid orders
def save_order(order):
    try:
        with open(
            "advanced_orders.jsonl",
            "a",
            encoding="utf-8"
        ) as file:
            file.write(json.dumps(order) + "\n")

        return True

    except OSError as error:
        print(f"File writing error: {error}")
        return False


# Main program
def main():
    order_number = 1000

    print("Starting E-Commerce Order Generator...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            order_number += 1

            order = generate_order(order_number)

            if validate_order(order):
                if save_order(order):
                    print(
                        f"Saved {order['order_id']} | "
                        f"{order['product_name']} | "
                        f"${order['total_amount']:.2f}"
                    )
                else:
                    print("Order could not be saved.")
            else:
                print("Invalid order detected!")

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nOrder generator stopped.")


if __name__ == "__main__":
    main()
