CREATE TABLE IF NOT EXISTS raw_sales (
    event_id VARCHAR(100) PRIMARY KEY,
    product VARCHAR(100),
    quantity INTEGER,
    price NUMERIC(10,2),
    event_timestamp TIMESTAMP,
    region VARCHAR(10)
);