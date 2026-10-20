
import json
import random
import time
from datetime import datetime, timezone

products = ["Laptop", "Phone", "Headphones", "Monitor"]

order_id = 1000

while True:
    order_id += 1

    product = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(50, 1000), 2)

    order = {
        "order_id": order_id,
        "product": product,
        "quantity": quantity,
        "price": price,
        "total_amount": round(quantity * price, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    print(json.dumps(order, indent=2))

    with open("orders.jsonl", "a", encoding="utf-8") as file:
        file.write(json.dumps(order) + "\n")

    time.sleep(2)
