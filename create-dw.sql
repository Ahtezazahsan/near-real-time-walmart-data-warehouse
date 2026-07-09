CREATE DATABASE IF NOT EXISTS walmart_dw;
USE walmart_dw;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_store;

CREATE TABLE dim_customer (
  customer_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(50) UNIQUE,
  gender VARCHAR(10),
  age_group VARCHAR(20),
  occupation VARCHAR(50),
  city_category VARCHAR(10),
  stay_in_current_city_years VARCHAR(5),
  marital_status INT
);

CREATE TABLE dim_product (
  product_sk INT AUTO_INCREMENT PRIMARY KEY,
  product_id VARCHAR(50) UNIQUE,
  product_category VARCHAR(100),
  price DECIMAL(10,2),
  store_id VARCHAR(20),
  supplier_id VARCHAR(20),
  store_name VARCHAR(100),
  supplier_name VARCHAR(100)
);

CREATE TABLE dim_store (
  store_sk INT AUTO_INCREMENT PRIMARY KEY,
  store_id VARCHAR(20) UNIQUE,
  store_name VARCHAR(100)
);

CREATE TABLE dim_date (
  date_sk INT AUTO_INCREMENT PRIMARY KEY,
  date DATE UNIQUE,
  year INT,
  month INT,
  day INT
);

CREATE TABLE fact_sales (
  sale_id INT AUTO_INCREMENT PRIMARY KEY,
  order_id VARCHAR(50),
  customer_sk INT,
  product_sk INT,
  date_sk INT,
  quantity INT,
  total_price DECIMAL(10,2),
  FOREIGN KEY(customer_sk) REFERENCES dim_customer(customer_sk),
  FOREIGN KEY(product_sk) REFERENCES dim_product(product_sk),
  FOREIGN KEY(date_sk) REFERENCES dim_date(date_sk),
  UNIQUE KEY uniq_fact_order (order_id, customer_sk, product_sk, date_sk)
);

-- Indexes
CREATE INDEX idx_fact_order ON fact_sales(order_id);
CREATE INDEX idx_dim_customer_cid ON dim_customer(customer_id);
CREATE INDEX idx_dim_product_pid ON dim_product(product_id);
CREATE INDEX idx_dim_date_date ON dim_date(date);
