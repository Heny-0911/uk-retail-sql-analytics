-- ============================================================
-- 1. SCHEMA DESIGN (DIMENSION + FACT TABLES)
-- ============================================================

CREATE TABLE customers (
    customer_id INTEGER,
    country TEXT
);

CREATE TABLE orders ( 
    order_id TEXT,
    order_date TEXT,
    customer_id INTEGER
);

CREATE TABLE products (
    product_id TEXT,
    product_name TEXT
);

CREATE TABLE order_items (
    order_id TEXT,
    product_id TEXT, 
    quantity INTEGER,
    unit_price REAL
);

-- ============================================================
-- 2. DATA INGESTION FROM STAGING TABLE
-- ============================================================

INSERT INTO customers
SELECT CustomerID, Country
FROM retail_staging
WHERE CustomerID IS NOT NULL;

INSERT INTO orders
SELECT InvoiceNo, InvoiceDate, CustomerID
FROM retail_staging
WHERE CustomerID IS NOT NULL;

INSERT INTO products
SELECT StockCode, Description
FROM retail_staging;

INSERT INTO order_items
SELECT InvoiceNo, StockCode, Quantity, UnitPrice
FROM retail_staging
WHERE Quantity > 0 AND UnitPrice > 0;

-- ============================================================
-- 3. DATA CLEANING & DEDUPLICATION
-- ============================================================

-- Clean Customers
DROP TABLE IF EXISTS customers_clean;

CREATE TABLE customers_clean AS
SELECT
    CustomerID AS customer_id,
    MIN(Country) AS country
FROM retail_staging
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID;

DROP TABLE customers;
ALTER TABLE customers_clean RENAME TO customers;


-- Clean Products
DROP TABLE IF EXISTS products_clean;

CREATE TABLE products_clean AS
SELECT
    StockCode AS product_id,
    MIN(Description) AS product_name
FROM retail_staging
GROUP BY StockCode;

DROP TABLE products;
ALTER TABLE products_clean RENAME TO products;


-- Clean Orders
DROP TABLE IF EXISTS orders_clean;

CREATE TABLE orders_clean AS
SELECT
    InvoiceNo AS order_id,
    MIN(InvoiceDate) AS order_date,
    CustomerID AS customer_id
FROM retail_staging
WHERE CustomerID IS NOT NULL
GROUP BY InvoiceNo, CustomerID;

DROP TABLE orders;
ALTER TABLE orders_clean RENAME TO orders;


-- Clean Order Items
DROP TABLE IF EXISTS order_items_clean;

CREATE TABLE order_items_clean AS
SELECT
    InvoiceNo AS order_id,
    StockCode AS product_id,
    Quantity AS quantity,
    UnitPrice AS unit_price
FROM retail_staging
WHERE Quantity > 0 AND UnitPrice > 0
GROUP BY InvoiceNo, StockCode, Quantity, UnitPrice;

DROP TABLE order_items;
ALTER TABLE order_items_clean RENAME TO order_items;

-- ============================================================
-- 4. CORE BUSINESS ANALYTICS
-- ============================================================

-- Total Revenue
SELECT 
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM order_items oi;


-- Revenue by Country
SELECT 
    c.country,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN orders o   ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.country
ORDER BY revenue DESC;


-- Top 5 Products by Revenue
SELECT 
    p.product_name,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;


-- Top 5 Customers by Revenue
SELECT 
    c.customer_id,
    c.country,
    SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN orders o   ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY revenue DESC
LIMIT 5;


-- ============================================================
-- 5. RFM CUSTOMER SEGMENTATION
-- ============================================================

-- Create RFM Table
DROP TABLE IF EXISTS customer_rfm;

CREATE TABLE customer_rfm AS
SELECT
    c.customer_id,
    c.country,
    MAX(o.order_date) AS last_purchase_date,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(oi.quantity * oi.unit_price) AS monetary
FROM customers c
LEFT JOIN orders o      ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.country;


-- Add RFM Scores
DROP TABLE IF EXISTS customer_rfm_scored;

CREATE TABLE customer_rfm_scored AS
SELECT *,
       CASE
           WHEN last_purchase_date >= DATE('now','-30 days') THEN 5
           WHEN last_purchase_date >= DATE('now','-60 days') THEN 4
           WHEN last_purchase_date >= DATE('now','-90 days') THEN 3
           WHEN last_purchase_date >= DATE('now','-120 days') THEN 2
           ELSE 1
       END AS recency_score,

       CASE
           WHEN frequency >= 20 THEN 5
           WHEN frequency >= 15 THEN 4
           WHEN frequency >= 10 THEN 3
           WHEN frequency >= 5  THEN 2
           ELSE 1
       END AS frequency_score,

       CASE
           WHEN monetary >= 1000 THEN 5
           WHEN monetary >= 500  THEN 4
           WHEN monetary >= 200  THEN 3
           WHEN monetary >= 100  THEN 2
           ELSE 1
       END AS monetary_score
FROM customer_rfm;


-- Create Customer Segments
DROP TABLE IF EXISTS customer_segments;

CREATE TABLE customer_segments AS
SELECT *,
       recency_score || frequency_score || monetary_score AS rfm_score,
       CASE
           WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champion'
           WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal'
           WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
           WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost'
           ELSE 'Need Attention'
       END AS segment
FROM customer_rfm_scored;


-- Revenue & Customer Count by Segment
SELECT 
    segment,
    COUNT(*) AS customers,
    SUM(monetary) AS revenue
FROM customer_segments
GROUP BY segment
ORDER BY revenue DESC;


-- ============================================================
-- 6. ADVANCED ANALYTICS (WINDOW FUNCTIONS & COHORT ANALYSIS)
-- ============================================================

-- Running (Cumulative) Revenue
SELECT
    DATE(InvoiceDate) AS order_date,
    SUM(Quantity * UnitPrice) AS daily_revenue,
    SUM(SUM(Quantity * UnitPrice)) OVER (
        ORDER BY DATE(InvoiceDate)
    ) AS cumulative_revenue
FROM retail_staging
WHERE Quantity > 0
GROUP BY DATE(InvoiceDate)
ORDER BY order_date;


-- Monthly Cohort Analysis
WITH first_purchase AS (
    SELECT
        CustomerID,
        MIN(DATE(InvoiceDate)) AS first_order_date
    FROM retail_staging
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID
),
cohort AS (
    SELECT
        o.CustomerID,
        STRFTIME('%Y-%m', f.first_order_date) AS cohort_month,
        STRFTIME('%Y-%m', o.InvoiceDate) AS order_month
    FROM retail_staging o
    JOIN first_purchase f ON o.CustomerID = f.CustomerID
)
SELECT
    cohort_month,
    order_month,
    COUNT(DISTINCT CustomerID) AS active_customers
FROM cohort
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;


-- Monthly Product Ranking
WITH product_sales AS (
    SELECT
        StockCode,
        STRFTIME('%Y-%m', InvoiceDate) AS month,
        SUM(Quantity * UnitPrice) AS revenue
    FROM retail_staging
    WHERE Quantity > 0
    GROUP BY StockCode, month
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY month ORDER BY revenue DESC) AS rank
    FROM product_sales
)
WHERE rank <= 5;


-- Pareto Analysis (80/20 Rule)
WITH customer_revenue AS (
    SELECT
        CustomerID,
        SUM(Quantity * UnitPrice) AS revenue
    FROM retail_staging
    WHERE Quantity > 0
    GROUP BY CustomerID
),
ranked AS (
    SELECT *,
           SUM(revenue) OVER () AS total_revenue,
           SUM(revenue) OVER (ORDER BY revenue DESC) AS running_revenue
    FROM customer_revenue
)
SELECT *,
       ROUND(running_revenue * 1.0 / total_revenue, 4) AS revenue_share
FROM ranked
ORDER BY revenue DESC;
