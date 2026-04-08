DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100)
);

CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(50)
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    brand VARCHAR(100),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100),
    rating DECIMAL(3,1),
    reviews INTEGER
);

CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name VARCHAR(200) UNIQUE NOT NULL,
    location VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200)
);

CREATE TABLE dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200) UNIQUE NOT NULL,
    contact VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_of_week_name VARCHAR(20)
);

CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    seller_key INTEGER REFERENCES dim_seller(seller_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    store_key INTEGER REFERENCES dim_store(store_key),
    supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    quantity INTEGER,
    total_price DECIMAL(10,2)
);