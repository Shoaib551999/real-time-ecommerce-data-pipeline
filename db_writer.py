from db_config import get_connection
from psycopg2.extras import execute_batch

     
def save_orders(orders):
    if not orders:
        return 0

    conn = get_connection()

    try:
        values = [
            (
                order["order_id"],
                order["customer_id"],
                order["product_id"],
                order["product_name"],
                order["category"],
                order["quantity"],
                order["unit_price"],
                order["total_amount"],
                order["payment_method"],
                order["order_status"],
                order["event_timestamp"],
            )
            for order in orders
        ]

        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                INSERT INTO orders (
                    order_id,
                    customer_id,
                    product_id,
                    product_name,
                    category,
                    quantity,
                    unit_price,
                    total_amount,
                    payment_method,
                    order_status,
                    event_timestamp
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO NOTHING
                """,
                values,
                page_size=100
            )

        conn.commit()
        return len(orders)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
