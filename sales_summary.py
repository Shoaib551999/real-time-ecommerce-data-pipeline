
import json

total_sales = 0
order_count = 0

with open("advanced_orders.jsonl", "r", encoding="utf-8") as file:
    for line in file:
        order = json.loads(line)

        if order["order_status"] == "PLACED":
            total_sales += order["total_amount"]
            order_count += 1

print(f"Placed orders: {order_count}")
print(f"Total placed order value: ${total_sales:.2f}")
