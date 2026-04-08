SELECT COUNT(*) AS fact_count FROM fact_sales;

SELECT 'source_raw' AS source, SUM(sale_total_price) AS total_sum FROM mock_data_raw
UNION ALL
SELECT 'fact_sales' AS source, SUM(total_price) FROM fact_sales;

SELECT 
    p.category,
    COUNT(*) AS sales_count,
    SUM(f.total_price) AS total_sales
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

SELECT 
    d.year,
    d.month_name,
    COUNT(*) AS transactions,
    SUM(f.total_price) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name, d.month
ORDER BY d.year, d.month;

SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(*) AS purchases,
    SUM(f.total_price) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.email
ORDER BY total_spent DESC
LIMIT 5;

SELECT 
    p.name,
    p.category,
    COUNT(*) AS times_sold,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_price) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.name, p.category
ORDER BY revenue DESC
LIMIT 5;