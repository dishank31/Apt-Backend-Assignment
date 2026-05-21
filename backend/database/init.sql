CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('pending', 'shipped', 'delivered')) DEFAULT 'pending',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_orders_modtime ON orders;

CREATE TRIGGER update_orders_modtime
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- Seed data
INSERT INTO orders (customer_name, product_name, status)
SELECT 'Dishank Gandhi', 'HP Victus Gaming Laptop', 'pending'
WHERE NOT EXISTS (
    SELECT 1
    FROM orders
    WHERE customer_name = 'Dishank Gandhi'
      AND product_name = 'HP Victus Gaming Laptop'
);


-- NOTE: The only trigger in the system is a utility trigger for automatically maintaining updated_at timestamps.