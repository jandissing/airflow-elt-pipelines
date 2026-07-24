-- Initialize PostgreSQL database with schema and sample data

-- Create raw_sales_data table
CREATE TABLE IF NOT EXISTS raw_sales_data (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    sale_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL
);

-- Create transformed_sales table
CREATE TABLE IF NOT EXISTS transformed_sales (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    sale_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample Brazilian sales data
INSERT INTO raw_sales_data (customer_name, product_name, quantity, unit_price, sale_date, region)
VALUES
    ('João Silva', 'Laptop', 1, 3500.00, '2024-01-15', 'Southeast'),
    ('Maria Santos', 'Mouse', 5, 150.00, '2024-01-16', 'South'),
    ('Carlos Oliveira', 'Keyboard', 2, 450.00, '2024-01-17', 'Northeast'),
    ('Ana Souza', 'Monitor', 1, 1200.00, '2024-01-18', 'North'),
    ('Pedro Costa', 'Headphones', 3, 350.00, '2024-01-19', 'Southeast'),
    ('Lucia Ferreira', 'Webcam', 4, 280.00, '2024-01-20', 'South'),
    ('Roberto Gomes', 'Laptop', 1, 3500.00, '2024-01-21', 'Northeast'),
    ('Fernanda Dias', 'Monitor', 2, 1200.00, '2024-01-22', 'North');

-- Create region_sales_summary table (populated by csv_to_db_dag: CSV -> aggregate by region -> DB)
CREATE TABLE IF NOT EXISTS region_sales_summary (
    region VARCHAR(50) PRIMARY KEY,
    total_quantity INTEGER,
    total_revenue DECIMAL(12, 2),
    avg_unit_price DECIMAL(10, 2),
    num_orders INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA public TO airflow_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO airflow_user;

-- Ensure future tables and sequences also grant permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO airflow_user;
