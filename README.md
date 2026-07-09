# Walmart Near-Real-Time Data Warehouse using HYBRIDJOIN

A near-real-time **Data Warehouse and Business Intelligence** project built for Walmart-style retail transactional data.  
The system implements an ETL pipeline that enriches streaming transactional records with master data, loads them into a star-schema data warehouse, and supports OLAP-style business analysis using SQL.

---

## Project Overview

Walmart generates large volumes of transactional data from stores and online purchases. Raw transaction records are often not enough for business intelligence because they need to be enriched with customer, product, store, supplier, and time-related attributes before analysis.

This project builds a prototype of a near-real-time data warehouse that:

- Reads transactional sales data as a continuous stream.
- Enriches each transaction using master data.
- Implements the **HYBRIDJOIN** stream-relation join algorithm in Python.
- Loads transformed records into a relational data warehouse.
- Supports OLAP analysis using SQL queries, slicing, dicing, drill-down, ROLLUP, and views.

---

## Key Features

- Near-real-time ETL pipeline for retail sales data.
- Python implementation of the **HYBRIDJOIN** algorithm.
- Stream buffer for incoming transactional data.
- Hash table and queue-based join processing.
- Disk-buffer-based partition loading from master data.
- Star-schema data warehouse design.
- SQL scripts for database and table creation.
- OLAP queries for business intelligence analysis.
- Analysis of revenue, products, customers, stores, suppliers, and time trends.
- Query optimization using SQL views.

---

## Technology Stack

| Category | Tools / Technologies |
|---|---|
| Programming | Python |
| Database | MySQL |
| Query Language | SQL, PL/SQL-style procedures/scripts |
| Data Engineering | ETL, Stream Processing, Data Enrichment |
| Data Warehouse | Star Schema, Fact Table, Dimension Tables |
| Analytics | OLAP Queries, Drill-down, Slicing, Dicing, ROLLUP |
| Testing | Python tests / SQL validation scripts |

---

## Dataset Files

The project uses three main data sources:

```text
customer_master_data.csv
product_master_data.csv
transactional_data.csv
```

### Data Role

| File | Purpose |
|---|---|
| `customer_master_data.csv` | Customer demographic and profile information |
| `product_master_data.csv` | Product, category, supplier, and product-related attributes |
| `transactional_data.csv` | Streaming transactional sales records |

---

## Repository Structure

Recommended structure for this repository:

```text
walmart-near-real-time-dw-hybridjoin/
│
├── README.md
├── requirements.txt
│
├── data/
│   ├── customer_master_data.csv
│   ├── product_master_data.csv
│   └── transactional_data.csv
│
├── sql/
│   ├── create-dw.sql
│   ├── create-test-dw.sql
│   └── analysis-queries.sql
│
├── src/
│   └── hybridjoin.py
│
├── tests/
│   └── test_hybridjoin.py
│
└── reports/
    └── project-report.pdf
```

If your local files are currently in the root folder, you can upload them as they are first, then organize them later.

---

## Data Warehouse Design

The project uses a star-schema design to support analytical reporting.

### Fact Table

The central fact table stores enriched sales transaction records.

Example:

```text
fact_sales
```

Possible measures:

- Purchase amount
- Quantity sold
- Revenue
- Transaction count
- Date key
- Customer key
- Product key
- Store key
- Supplier key

### Dimension Tables

Example dimensions include:

```text
dim_customer
dim_product
dim_store
dim_supplier
dim_date
```

These dimensions support analysis by:

- Gender
- Age group
- City category
- Marital status
- Product category
- Occupation
- Supplier
- Store
- Month
- Quarter
- Season
- Weekday / Weekend

---

## HYBRIDJOIN Algorithm

HYBRIDJOIN is used to join a fast-arriving stream of transactions with a larger disk-based master relation.

### Main Components

| Component | Description |
|---|---|
| Stream Buffer | Temporarily stores incoming transaction records |
| Hash Table | Stores stream tuples using the join key |
| Queue | Maintains FIFO order of stream keys |
| Disk Buffer | Loads partitions of master data for joining |
| Join Output | Enriched records loaded into the data warehouse |

### Processing Flow

1. A stream reader thread continuously reads transaction records.
2. Incoming transaction records are placed into the stream buffer.
3. The HYBRIDJOIN thread loads available stream records into the hash table.
4. Join keys are inserted into a FIFO queue.
5. The oldest queue key is used to load a relevant partition from master data.
6. The disk partition is probed against the hash table.
7. Matching records are enriched and inserted into the data warehouse.
8. Matched stream records are removed from the hash table and queue.
9. The process repeats until the stream is processed.

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/walmart-near-real-time-dw-hybridjoin.git
cd walmart-near-real-time-dw-hybridjoin
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
```

Activate it:

```bash
# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 3. Install Requirements

```bash
pip install -r requirements.txt
```

Example `requirements.txt`:

```text
pandas
mysql-connector-python
numpy
```

---

## Database Setup

### 1. Open MySQL

```bash
mysql -u root -p
```

### 2. Run the Data Warehouse Creation Script

```bash
mysql -u root -p < sql/create-dw.sql
```

This script creates the data warehouse schema, dimension tables, fact table, primary keys, and foreign keys.

---

## Running the HYBRIDJOIN ETL Pipeline

Run the Python implementation:

```bash
python src/hybridjoin.py
```

The script should ask for database credentials at runtime, such as:

```text
Host:
User:
Password:
Database:
```

After successful execution, the enriched transaction records will be loaded into the data warehouse fact table.

---

## Running OLAP Analysis Queries

After loading data into the warehouse, run the analysis queries:

```bash
mysql -u root -p walmart_dw < sql/analysis-queries.sql
```

---

## Business Intelligence Analysis

The project supports multiple analytical queries, including:

1. Top revenue-generating products on weekdays and weekends.
2. Customer demographics by purchase amount.
3. Product category sales by occupation.
4. Purchases by gender and age group with quarterly trends.
5. Top occupations by product category sales.
6. City category performance by marital status.
7. Average purchase amount by stay duration and gender.
8. Top revenue-generating cities by product category.
9. Monthly sales growth by product category.
10. Weekend vs weekday sales by age group.
11. Quarterly store revenue growth.
12. Supplier sales contribution by store and product.
13. Seasonal product sales drill-down.
14. Store-wise and supplier-wise revenue volatility.
15. Product affinity analysis.
16. Yearly revenue trends using ROLLUP.
17. H1 vs H2 revenue and volume analysis.
18. Revenue spike and outlier detection.
19. Optimized quarterly sales view.

---

## Example Use Cases

This project can help retail businesses answer questions such as:

- Which products generate the most revenue on weekends?
- Which customer groups spend the most?
- Which product categories are popular among different occupations?
- Which stores show strong quarterly growth?
- Which suppliers contribute most to sales?
- Which products are commonly purchased together?
- Which products show unusual sales spikes?

---

## Outcomes

- Designed and implemented a star-schema data warehouse.
- Built a near-real-time ETL pipeline using Python.
- Implemented HYBRIDJOIN for stream-relation data enrichment.
- Loaded enriched transactional data into a MySQL warehouse.
- Created SQL analytical queries for business intelligence reporting.
- Improved understanding of data warehousing, OLAP analysis, and real-time ETL processing.

---

## Future Improvements

- Add Apache Kafka for real streaming ingestion.
- Add Apache Spark for large-scale batch and stream processing.
- Build Power BI or Tableau dashboards on top of the warehouse.
- Add Docker support for easier deployment.
- Add automated data quality checks.
- Add CI/CD testing for SQL and Python scripts.
- Optimize partition loading and indexing strategies.

---

## Author

**Ahtezaz Ahsan Khan**  
BS Data Science, FAST-NUCES Islamabad  
Data Engineer | ETL Developer | Data Analyst

---

## Disclaimer

This project was developed for academic and portfolio purposes to demonstrate data warehousing, ETL, HYBRIDJOIN, SQL analytics, and business intelligence concepts.
