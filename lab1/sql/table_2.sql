INSERT INTO dim_customer (
    customer_id, first_name, last_name, age, email, 
    country, postal_code, pet_type, pet_name, pet_breed
)
SELECT DISTINCT 
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data_raw
WHERE sale_customer_id IS NOT NULL
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO dim_seller (
    seller_id, first_name, last_name, email, country, postal_code
)
SELECT DISTINCT 
    sale_seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data_raw
WHERE sale_seller_id IS NOT NULL
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO dim_product (
    product_id, name, category, price, brand, 
    weight, color, size, material, rating, reviews
)
SELECT DISTINCT 
    sale_product_id,
    product_name,
    product_category,
    product_price,
    product_brand,
    product_weight,
    product_color,
    product_size,
    product_material,
    product_rating,
    product_reviews
FROM mock_data_raw
WHERE sale_product_id IS NOT NULL
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO dim_store (
    store_name, location, city, state, country, phone, email
)
SELECT DISTINCT 
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data_raw
WHERE store_name IS NOT NULL
ON CONFLICT (store_name) DO NOTHING;

INSERT INTO dim_supplier (
    supplier_name, contact, email, phone, address, city, country
)
SELECT DISTINCT 
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data_raw
WHERE supplier_name IS NOT NULL
ON CONFLICT (supplier_name) DO NOTHING;

INSERT INTO dim_date (
    full_date, year, quarter, month, month_name, 
    day, day_of_week, day_of_week_name
)
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/DD/YYYY') AS full_date,
    EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS year,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS quarter,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS month,
    TO_CHAR(TO_DATE(sale_date, 'MM/DD/YYYY'), 'Month') AS month_name,
    EXTRACT(DAY FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS day,
    EXTRACT(DOW FROM TO_DATE(sale_date, 'MM/DD/YYYY')) AS day_of_week,
    TO_CHAR(TO_DATE(sale_date, 'MM/DD/YYYY'), 'Day') AS day_of_week_name
FROM mock_data_raw
WHERE sale_date IS NOT NULL
ON CONFLICT (full_date) DO NOTHING;

INSERT INTO fact_sales (
    customer_key, seller_key, product_key, store_key, supplier_key, date_key,
    quantity, total_price
)
SELECT
    c.customer_key,
    sel.seller_key,
    p.product_key,
    s.store_key,
    sup.supplier_key,
    d.date_key,
    r.sale_quantity,
    r.sale_total_price
FROM mock_data_raw r
LEFT JOIN dim_customer c ON r.sale_customer_id = c.customer_id
LEFT JOIN dim_seller sel ON r.sale_seller_id = sel.seller_id
LEFT JOIN dim_product p ON r.sale_product_id = p.product_id
LEFT JOIN dim_store s ON r.store_name = s.store_name
LEFT JOIN dim_supplier sup ON r.supplier_name = sup.supplier_name
LEFT JOIN dim_date d ON TO_DATE(r.sale_date, 'MM/DD/YYYY') = d.full_date;