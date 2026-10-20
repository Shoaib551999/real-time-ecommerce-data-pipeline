from db_config import get_connection

conn = get_connection()
cursor = conn.cursor()

cursor.execute("""
    INSERT INTO orders (
        order_id, customer_id, product_id, product_name,
        category, quantity, unit_price, total_amount,
        payment_method, order_status, event_timestamp
    )
    VALUES (
        'TEST-DB-001', 'CUST-TEST', 'PROD-TEST', 'Laptop',
        'Electronics', 1, 750.00, 750.00,
        'Credit Card', 'CONFIRMED', NOW()
    )
    ON CONFLICT (order_id) DO NOTHING;
""")

conn.commit()

cursor.execute(
    "SELECT order_id, product_name, total_amount FROM orders WHERE order_id = %s",
    ("TEST-DB-001",)
)

print("Saved order:", cursor.fetchone())

cursor.close()
conn.close()