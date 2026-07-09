USE walmart_dw;

-- ============================================================================
-- HELPER QUERY: Check available date range in your data
-- Run this first to see what years/months have data
-- ============================================================================
/*
SELECT 
    MIN(dd.date) AS earliest_date,
    MAX(dd.date) AS latest_date,
    MIN(dd.year) AS earliest_year,
    MAX(dd.year) AS latest_year,
    COUNT(DISTINCT dd.date) AS unique_dates,
    COUNT(*) AS total_fact_records
FROM fact_sales fs
JOIN dim_date dd ON fs.date_sk = dd.date_sk
WHERE dd.date IS NOT NULL;
*/

-- ============================================================================
-- Global parameters to keep the analysis flexible
-- ============================================================================
-- IMPORTANT: Adjust these based on your actual data range
-- Run the helper query above first to see available dates
SET @analysis_year = 2017;  -- Year for Q1, Q11 analysis
SET @current_year = 2017;  -- Changed from YEAR(CURDATE()) since data is from 2017-2020
SET @q11_year = 2017;  -- Year for Q11 analysis

-- Fix: Use actual data range instead of CURDATE() which would be 2024
-- Data is from 2017-2020, so use 6 months from end of available data
-- Adjust these if your data has different date ranges
SET @six_month_end = '2020-12-31';  -- End of available data range
SET @six_month_start = DATE_SUB(@six_month_end, INTERVAL 6 MONTH);  -- Last 6 months of available data

-- Q1. Top 5 products by revenue for weekdays vs weekends with monthly drill-down
WITH product_monthly AS (
  SELECT
    dp.product_id,
    dp.product_category,
    dp.store_name,
    MONTH(dd.date) AS month_num,
    DATE_FORMAT(dd.date, '%Y-%m') AS month_label,
    CASE WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(fs.total_price) AS revenue
  FROM fact_sales fs
  JOIN dim_product dp ON fs.product_sk = dp.product_sk
  JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.year = @analysis_year
  GROUP BY dp.product_id, dp.product_category, dp.store_name, month_num, month_label, day_type
),
ranked AS (
  SELECT
    product_id,
    product_category,
    store_name,
    month_label,
    day_type,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY month_label, day_type ORDER BY revenue DESC) AS rn
  FROM product_monthly
)
SELECT product_id, product_category, store_name, month_label, day_type, revenue
FROM ranked
WHERE rn <= 5
ORDER BY month_label, day_type, revenue DESC;

-- Q2. Customer demographics by purchase amount with city category breakdown
SELECT
  dc.gender,
  dc.age_group,
  dc.city_category,
  SUM(fs.total_price) AS total_purchase
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
GROUP BY dc.gender, dc.age_group, dc.city_category
ORDER BY dc.gender, dc.age_group, total_purchase DESC;

-- Q3. Product category sales by occupation
SELECT
  dc.occupation,
  dp.product_category,
  SUM(fs.total_price) AS total_sales
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
JOIN dim_product dp ON fs.product_sk = dp.product_sk
GROUP BY dc.occupation, dp.product_category
ORDER BY dc.occupation, total_sales DESC;

-- Q4. Total purchases by gender and age group with quarterly trend for the current year
-- NOTE: @current_year is set to 2017 (year with available data)
-- You can change @current_year to any year 2017-2020 to see different results
WITH quarterly AS (
  SELECT
    COALESCE(dc.gender, 'Unknown') AS gender,
    COALESCE(dc.age_group, 'Unknown') AS age_group,
    QUARTER(dd.date) AS quarter_num,
    SUM(COALESCE(fs.total_price, 0)) AS total_purchase
  FROM fact_sales fs
  INNER JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
  INNER JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.year = @current_year
    AND dd.date IS NOT NULL
    AND fs.total_price IS NOT NULL
    AND COALESCE(dc.gender, '') != ''
    AND COALESCE(dc.age_group, '') != ''
  GROUP BY COALESCE(dc.gender, 'Unknown'), COALESCE(dc.age_group, 'Unknown'), quarter_num
  HAVING SUM(COALESCE(fs.total_price, 0)) > 0
),
quarterly_with_lag AS (
  SELECT
    gender,
    age_group,
    quarter_num,
    CONCAT('Q', quarter_num) AS quarter_label,
    total_purchase,
    COALESCE(LAG(total_purchase) OVER (PARTITION BY gender, age_group ORDER BY quarter_num), 0) AS prev_quarter
  FROM quarterly
)
SELECT
  gender,
  age_group,
  quarter_label,
  ROUND(total_purchase, 2) AS total_purchase,
  ROUND(prev_quarter, 2) AS prev_quarter,
  CASE
    WHEN prev_quarter = 0 THEN 0
    ELSE ROUND((total_purchase - prev_quarter) / prev_quarter * 100, 2)
  END AS qoq_change_pct
FROM quarterly_with_lag
ORDER BY gender, age_group, quarter_num;

-- Q5. Top occupations by product category sales (top 5 per category)
WITH occupation_sales AS (
  SELECT
    dp.product_category,
    dc.occupation,
    SUM(fs.total_price) AS total_sales
  FROM fact_sales fs
  JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
  JOIN dim_product dp ON fs.product_sk = dp.product_sk
  GROUP BY dp.product_category, dc.occupation
),
ranked AS (
  SELECT
    product_category,
    occupation,
    total_sales,
    ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY total_sales DESC) AS rn
  FROM occupation_sales
)
SELECT product_category, occupation, total_sales
FROM ranked
WHERE rn <= 5
ORDER BY product_category, total_sales DESC;

-- Q6. City category performance by marital status with monthly breakdown (past 6 months)
-- NOTE: Uses last 6 months of available data (2020-07-01 to 2020-12-31)
-- You can adjust @six_month_end and @six_month_start to analyze different periods
SELECT
  dc.city_category,
  dc.marital_status,
  DATE_FORMAT(dd.date, '%Y-%m') AS month_label,
  SUM(fs.total_price) AS total_purchase,
  COUNT(DISTINCT fs.order_id) AS order_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
JOIN dim_date dd ON fs.date_sk = dd.date_sk
WHERE dd.date IS NOT NULL  -- Ensure date_sk is resolved (not NULL)
  AND dd.date BETWEEN @six_month_start AND @six_month_end
GROUP BY dc.city_category, dc.marital_status, month_label
ORDER BY month_label, dc.city_category, dc.marital_status;

-- Q7. Average purchase amount by stay duration and gender
SELECT
  dc.stay_in_current_city_years,
  dc.gender,
  AVG(fs.total_price) AS avg_purchase_amount,
  COUNT(*) AS order_count
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
GROUP BY dc.stay_in_current_city_years, dc.gender
ORDER BY dc.stay_in_current_city_years, dc.gender;

-- Q8. Top 5 revenue-generating city categories by product category
WITH city_category_sales AS (
  SELECT
    dp.product_category,
    dc.city_category,
    SUM(fs.total_price) AS total_revenue
  FROM fact_sales fs
  JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
  JOIN dim_product dp ON fs.product_sk = dp.product_sk
  GROUP BY dp.product_category, dc.city_category
),
ranked AS (
  SELECT
    product_category,
    city_category,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY total_revenue DESC) AS rn
  FROM city_category_sales
)
SELECT product_category, city_category, total_revenue
FROM ranked
WHERE rn <= 5
ORDER BY product_category, total_revenue DESC;

-- Q9. Monthly sales growth percentage by product category for the current year
-- NOTE: @current_year is set to 2017 (year with available data)
WITH monthly_sales AS (
  SELECT
    COALESCE(dp.product_category, 'Unknown') AS product_category,
    MONTH(dd.date) AS month_num,
    DATE_FORMAT(dd.date, '%Y-%m') AS month_label,
    SUM(COALESCE(fs.total_price, 0)) AS revenue
  FROM fact_sales fs
  INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
  INNER JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.year = @current_year
    AND dd.date IS NOT NULL
    AND fs.total_price IS NOT NULL
  GROUP BY COALESCE(dp.product_category, 'Unknown'), month_num, month_label
  HAVING SUM(COALESCE(fs.total_price, 0)) > 0
),
monthly_with_lag AS (
  SELECT
    product_category,
    month_num,
    month_label,
    revenue,
    COALESCE(LAG(revenue) OVER (PARTITION BY product_category ORDER BY month_num), 0) AS prev_month_revenue
  FROM monthly_sales
)
SELECT
  product_category,
  month_label,
  ROUND(revenue, 2) AS revenue,
  ROUND(prev_month_revenue, 2) AS prev_month_revenue,
  CASE
    WHEN prev_month_revenue = 0 THEN 0
    ELSE ROUND((revenue - prev_month_revenue) / prev_month_revenue * 100, 2)
  END AS growth_pct
FROM monthly_with_lag
ORDER BY product_category, month_num;

-- Q10. Weekend vs weekday sales by age group for the current year
-- NOTE: @current_year is set to 2017 (year with available data)
SELECT
  dc.age_group,
  CASE WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  SUM(fs.total_price) AS total_sales
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
JOIN dim_date dd ON fs.date_sk = dd.date_sk
WHERE dd.year = @current_year
  AND dd.date IS NOT NULL  -- Ensure date_sk is resolved (not NULL)
GROUP BY dc.age_group, day_type
ORDER BY dc.age_group, day_type;

-- Q11. Top revenue-generating products on weekdays and weekends with monthly drill-down (specified year)
WITH product_split AS (
  SELECT
    dp.product_id,
    dp.product_category,
    DATE_FORMAT(dd.date, '%Y-%m') AS month_label,
    CASE WHEN DAYOFWEEK(dd.date) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(fs.total_price) AS revenue
  FROM fact_sales fs
  JOIN dim_product dp ON fs.product_sk = dp.product_sk
  JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.year = @q11_year
  GROUP BY dp.product_id, dp.product_category, month_label, day_type
),
ranked AS (
  SELECT
    product_id,
    product_category,
    month_label,
    day_type,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY month_label, day_type ORDER BY revenue DESC) AS rn
  FROM product_split
)
SELECT product_id, product_category, month_label, day_type, revenue
FROM ranked
WHERE rn <= 5
ORDER BY month_label, day_type, revenue DESC;

-- Q12. Quarterly store revenue growth rate for 2017
WITH store_quarterly AS (
  SELECT
    COALESCE(dp.store_id, 'Unknown') AS store_id,
    COALESCE(dp.store_name, 'Unknown') AS store_name,
    QUARTER(dd.date) AS quarter_num,
    CONCAT(dd.year, '-Q', QUARTER(dd.date)) AS quarter_label,
    SUM(COALESCE(fs.total_price, 0)) AS revenue
  FROM fact_sales fs
  INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
  INNER JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.year = 2017
    AND dd.date IS NOT NULL
    AND fs.total_price IS NOT NULL
  GROUP BY COALESCE(dp.store_id, 'Unknown'), COALESCE(dp.store_name, 'Unknown'), quarter_num, quarter_label
  HAVING SUM(COALESCE(fs.total_price, 0)) > 0
),
store_quarterly_with_lag AS (
  SELECT
    store_id,
    store_name,
    quarter_num,
    quarter_label,
    revenue,
    COALESCE(LAG(revenue) OVER (PARTITION BY store_id ORDER BY quarter_num), 0) AS prev_quarter_revenue
  FROM store_quarterly
)
SELECT
  store_id,
  store_name,
  quarter_label,
  ROUND(revenue, 2) AS revenue,
  ROUND(prev_quarter_revenue, 2) AS prev_quarter_revenue,
  CASE
    WHEN prev_quarter_revenue = 0 THEN 0
    ELSE ROUND((revenue - prev_quarter_revenue) / prev_quarter_revenue * 100, 2)
  END AS growth_rate_pct
FROM store_quarterly_with_lag
ORDER BY store_name, quarter_num;

-- Q13. Supplier sales contribution by store and product name
SELECT
  dp.store_name,
  dp.store_id,
  dp.supplier_name,
  dp.supplier_id,
  dp.product_id,
  dp.product_category,
  SUM(fs.total_price) AS total_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_sk = dp.product_sk
GROUP BY dp.store_name, dp.store_id, dp.supplier_name, dp.supplier_id, dp.product_id, dp.product_category
ORDER BY dp.store_name, dp.supplier_name, total_sales DESC;

-- Q14. Seasonal analysis of product sales using dynamic drill-down
SELECT
  dp.product_id,
  dp.product_category,
  CASE
    WHEN MONTH(dd.date) IN (12, 1, 2) THEN 'Winter'
    WHEN MONTH(dd.date) IN (3, 4, 5) THEN 'Spring'
    WHEN MONTH(dd.date) IN (6, 7, 8) THEN 'Summer'
    ELSE 'Fall'
  END AS season,
  SUM(fs.total_price) AS total_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_sk = dp.product_sk
JOIN dim_date dd ON fs.date_sk = dd.date_sk
GROUP BY dp.product_id, dp.product_category, season
ORDER BY dp.product_id, season;

-- Q15. Store-wise and supplier-wise monthly revenue volatility
WITH monthly_store_supplier AS (
  SELECT
    COALESCE(dp.store_id, 'Unknown') AS store_id,
    COALESCE(dp.store_name, 'Unknown') AS store_name,
    COALESCE(dp.supplier_id, 'Unknown') AS supplier_id,
    COALESCE(dp.supplier_name, 'Unknown') AS supplier_name,
    YEAR(dd.date) AS year_val,
    MONTH(dd.date) AS month_num,
    DATE_FORMAT(dd.date, '%Y-%m') AS month_label,
    SUM(COALESCE(fs.total_price, 0)) AS revenue
  FROM fact_sales fs
  INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
  INNER JOIN dim_date dd ON fs.date_sk = dd.date_sk
  WHERE dd.date IS NOT NULL
    AND fs.total_price IS NOT NULL
  GROUP BY COALESCE(dp.store_id, 'Unknown'), COALESCE(dp.store_name, 'Unknown'), 
           COALESCE(dp.supplier_id, 'Unknown'), COALESCE(dp.supplier_name, 'Unknown'), 
           year_val, month_num, month_label
  HAVING SUM(COALESCE(fs.total_price, 0)) > 0
),
monthly_with_lag AS (
  SELECT
    store_id,
    store_name,
    supplier_id,
    supplier_name,
    year_val,
    month_num,
    month_label,
    revenue,
    COALESCE(LAG(revenue) OVER (PARTITION BY store_id, supplier_id ORDER BY year_val, month_num), 0) AS prev_month_revenue
  FROM monthly_store_supplier
)
SELECT
  store_name,
  supplier_name,
  month_label,
  ROUND(revenue, 2) AS revenue,
  ROUND(prev_month_revenue, 2) AS prev_month_revenue,
  CASE
    WHEN prev_month_revenue = 0 THEN 0
    ELSE ROUND((revenue - prev_month_revenue) / prev_month_revenue * 100, 2)
  END AS volatility_pct
FROM monthly_with_lag
ORDER BY store_name, supplier_name, year_val, month_num;

-- Q16. Top 5 products purchased together (product affinity)
-- Strategy: If orders have multiple products, use same order. Otherwise, use same customer + same date
WITH orders_with_multiple_products AS (
  -- Check if there are orders with multiple products
  SELECT 
    fs.order_id,
    COUNT(DISTINCT fs.product_sk) AS product_count
  FROM fact_sales fs
  WHERE fs.order_id IS NOT NULL
    AND fs.product_sk IS NOT NULL
  GROUP BY fs.order_id
  HAVING COUNT(DISTINCT fs.product_sk) > 1
),
-- Try approach 1: Products in same order (if orders have multiple products)
same_order_pairs AS (
  SELECT
    CASE WHEN op1.product_id < op2.product_id THEN op1.product_id ELSE op2.product_id END AS product_a,
    CASE WHEN op1.product_id < op2.product_id THEN op2.product_id ELSE op1.product_id END AS product_b,
    COUNT(DISTINCT op1.order_id) AS together_count,
    'Same Order' AS pair_type
  FROM (
    SELECT DISTINCT
      om.order_id,
      fs.product_sk,
      dp.product_id
    FROM orders_with_multiple_products om
    INNER JOIN fact_sales fs ON om.order_id = fs.order_id
    INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
    WHERE dp.product_id IS NOT NULL AND dp.product_id != ''
  ) op1
  INNER JOIN (
    SELECT DISTINCT
      om.order_id,
      fs.product_sk,
      dp.product_id
    FROM orders_with_multiple_products om
    INNER JOIN fact_sales fs ON om.order_id = fs.order_id
    INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
    WHERE dp.product_id IS NOT NULL AND dp.product_id != ''
  ) op2
    ON op1.order_id = op2.order_id
   AND op1.product_id != op2.product_id
  GROUP BY 
    CASE WHEN op1.product_id < op2.product_id THEN op1.product_id ELSE op2.product_id END,
    CASE WHEN op1.product_id < op2.product_id THEN op2.product_id ELSE op1.product_id END
),
-- Approach 2: Products purchased by same customer on same date (fallback)
same_customer_date_pairs AS (
  SELECT
    CASE WHEN p1.product_id < p2.product_id THEN p1.product_id ELSE p2.product_id END AS product_a,
    CASE WHEN p1.product_id < p2.product_id THEN p2.product_id ELSE p1.product_id END AS product_b,
    COUNT(DISTINCT CONCAT(p1.customer_sk, '-', p1.date_sk)) AS together_count,
    'Same Customer & Date' AS pair_type
  FROM (
    SELECT DISTINCT
      fs.customer_sk,
      fs.date_sk,
      fs.product_sk,
      dp.product_id
    FROM fact_sales fs
    INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
    WHERE fs.customer_sk IS NOT NULL
      AND fs.date_sk IS NOT NULL
      AND dp.product_id IS NOT NULL
      AND dp.product_id != ''
  ) p1
  INNER JOIN (
    SELECT DISTINCT
      fs.customer_sk,
      fs.date_sk,
      fs.product_sk,
      dp.product_id
    FROM fact_sales fs
    INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
    WHERE fs.customer_sk IS NOT NULL
      AND fs.date_sk IS NOT NULL
      AND dp.product_id IS NOT NULL
      AND dp.product_id != ''
  ) p2
    ON p1.customer_sk = p2.customer_sk
   AND p1.date_sk = p2.date_sk
   AND p1.product_id != p2.product_id
  GROUP BY 
    CASE WHEN p1.product_id < p2.product_id THEN p1.product_id ELSE p2.product_id END,
    CASE WHEN p1.product_id < p2.product_id THEN p2.product_id ELSE p1.product_id END
),
-- Combine both approaches, preferring same-order pairs
all_pairs AS (
  SELECT product_a, product_b, together_count, pair_type
  FROM same_order_pairs
  WHERE together_count > 0
  
  UNION ALL
  
  SELECT product_a, product_b, together_count, pair_type
  FROM same_customer_date_pairs
  WHERE together_count > 0
    AND (product_a, product_b) NOT IN (
      SELECT product_a, product_b FROM same_order_pairs WHERE together_count > 0
    )
),
-- Aggregate and rank
ranked_pairs AS (
  SELECT
    product_a,
    product_b,
    SUM(together_count) AS total_together_count,
    GROUP_CONCAT(DISTINCT pair_type ORDER BY pair_type SEPARATOR ', ') AS pair_types
  FROM all_pairs
  GROUP BY product_a, product_b
)
SELECT 
  product_a, 
  product_b, 
  total_together_count AS together_count,
  pair_types
FROM ranked_pairs
WHERE total_together_count > 0
ORDER BY total_together_count DESC
LIMIT 5;

-- Q17. Yearly revenue trends by store, supplier, and product with ROLLUP
SELECT
  CASE WHEN GROUPING(dp.store_name) = 1 THEN 'ALL STORES' ELSE dp.store_name END AS store_name,
  CASE WHEN GROUPING(dp.supplier_name) = 1 THEN 'ALL SUPPLIERS' ELSE dp.supplier_name END AS supplier_name,
  CASE WHEN GROUPING(dp.product_id) = 1 THEN 'ALL PRODUCTS' ELSE dp.product_id END AS product_id,
  dd.year,
  SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_sk = dp.product_sk
JOIN dim_date dd ON fs.date_sk = dd.date_sk
GROUP BY dd.year, dp.store_name, dp.supplier_name, dp.product_id WITH ROLLUP
ORDER BY dd.year, dp.store_name, dp.supplier_name, dp.product_id;

-- Q18. Revenue and volume-based sales analysis for each product (H1 vs H2)
SELECT
  dp.product_id,
  dp.product_category,
  SUM(CASE WHEN MONTH(dd.date) BETWEEN 1 AND 6 THEN fs.total_price ELSE 0 END) AS revenue_h1,
  SUM(CASE WHEN MONTH(dd.date) BETWEEN 7 AND 12 THEN fs.total_price ELSE 0 END) AS revenue_h2,
  SUM(fs.total_price) AS revenue_year_total,
  SUM(CASE WHEN MONTH(dd.date) BETWEEN 1 AND 6 THEN fs.quantity ELSE 0 END) AS quantity_h1,
  SUM(CASE WHEN MONTH(dd.date) BETWEEN 7 AND 12 THEN fs.quantity ELSE 0 END) AS quantity_h2,
  SUM(fs.quantity) AS quantity_year_total
FROM fact_sales fs
JOIN dim_product dp ON fs.product_sk = dp.product_sk
JOIN dim_date dd ON fs.date_sk = dd.date_sk
GROUP BY dp.product_id, dp.product_category
ORDER BY dp.product_id;

-- Q19. Identify high revenue spikes in product sales and highlight outliers
WITH daily_sales AS (
  SELECT
    dp.product_id,
    dd.date,
    SUM(fs.total_price) AS daily_revenue
  FROM fact_sales fs
  JOIN dim_product dp ON fs.product_sk = dp.product_sk
  JOIN dim_date dd ON fs.date_sk = dd.date_sk
  GROUP BY dp.product_id, dd.date
),
daily_avg AS (
  SELECT
    product_id,
    AVG(daily_revenue) AS avg_daily_revenue
  FROM daily_sales
  GROUP BY product_id
)
SELECT
  ds.product_id,
  ds.date,
  ds.daily_revenue,
  da.avg_daily_revenue,
  CASE WHEN ds.daily_revenue >= 2 * da.avg_daily_revenue THEN 'Spike' ELSE 'Normal' END AS spike_flag
FROM daily_sales ds
JOIN daily_avg da ON ds.product_id = da.product_id
WHERE ds.daily_revenue >= 2 * da.avg_daily_revenue
ORDER BY ds.daily_revenue DESC;

-- Q20. Create view STORE_QUARTERLY_SALES for optimized store-level analysis
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
  COALESCE(dp.store_name, 'Unknown') AS store_name,
  COALESCE(dp.store_id, 'Unknown') AS store_id,
  dd.year,
  QUARTER(dd.date) AS quarter_num,
  CONCAT(dd.year, '-Q', QUARTER(dd.date)) AS quarter_label,
  SUM(COALESCE(fs.total_price, 0)) AS total_revenue,
  SUM(COALESCE(fs.quantity, 0)) AS total_quantity,
  COUNT(DISTINCT fs.order_id) AS order_count
FROM fact_sales fs
INNER JOIN dim_product dp ON fs.product_sk = dp.product_sk
INNER JOIN dim_date dd ON fs.date_sk = dd.date_sk
WHERE dd.date IS NOT NULL
  AND fs.total_price IS NOT NULL
GROUP BY COALESCE(dp.store_name, 'Unknown'), COALESCE(dp.store_id, 'Unknown'), dd.year, quarter_num, quarter_label;

-- Query the view to verify it works and show results
SELECT * FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter_num
LIMIT 100;

