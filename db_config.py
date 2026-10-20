import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        host="172.19.80.1",
        port=5433,
        database="ecommerce",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5
    )