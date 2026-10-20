-- Real-Time E-Commerce Database Schema

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    quantity INTEGER,
    unit_price NUMERIC,
    total_amount NUMERIC,
    payment_method VARCHAR,
    order_status VARCHAR,
    event_timestamp TIMESTAMPTZ
);