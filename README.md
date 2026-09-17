# E-Commerce Data Engineering Pipeline

## Overview

An end-to-end data engineering pipeline for an e-commerce company, built using AWS S3, Databricks, Unity Catalog, Delta Lake, PySpark, SQL, and Power BI.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) to ingest raw e-commerce data, perform data cleansing and transformation, build analytics-ready fact and dimension tables, and provide business insights through Power BI dashboards.

## Project Objective

The objective of this project is to build a centralized and automated data platform for an e-commerce company.

### Key Objectives

- Centralize data from multiple source files and business domains
- Automate data ingestion and transformation
- Implement data cleansing and quality checks
- Build analytical fact and dimension tables
- Provide a single source of truth for reporting
- Enable business analysis through Power BI

## Architecture

The pipeline uses AWS S3 as the underlying cloud storage layer and Databricks as the data processing platform.

Unity Catalog is used to organize and govern the data through a centralized `ecommerce` catalog containing separate schemas for raw, Bronze, Silver, and Gold data.

The managed Delta tables created through Unity Catalog are physically stored in AWS S3, while Databricks performs the ingestion and transformation processing.

```text
Source System
     │
     │ CSV Files
     ▼
  AWS S3
     │
     │ IAM Role + S3 Policy
     ▼
 Databricks
     │
     │ Unity Catalog
     ▼
  ecommerce
     │
     ├── raw
     │
     ├── bronze
     │
     ├── silver
     │
     └── gold
          │
          ▼
      Power BI
```
## Technology Stack

| Technology | Purpose |
|------------|---------|
| **AWS S3** | Cloud object storage for raw source files and managed Delta table data |
| **AWS IAM** | Secure access to S3 using IAM roles and policies |
| **Databricks** | Data ingestion, processing, and transformation |
| **Unity Catalog** | Centralized data organization, governance, and access management |
| **Delta Lake** | Reliable storage and transaction management for processed tables |
| **PySpark** | Data transformation and processing |
| **Power BI** | Business reporting and data visualization |

## Data Sources & Ingestion

The project uses CSV files containing dimensional and transactional e-commerce data. For the initial historical load, the source files were uploaded to an AWS S3 bucket and organized under the `historical-full-load` directory.

### Source Data

The source datasets are divided into two categories:

**Dimension Data**
- Products
- Brands
- Categories
- Customers
- Date / Calendar

**Fact Data**
- Order Items
- Order Returns
- Order Shipments

### S3 Landing Structure

AWS S3 acts as the cloud storage layer for the source data.

```text
AWS S3
│
└── historical-full-load/
    │
    ├── Dimension Data
    │   ├── products
    │   ├── brands
    │   ├── category
    │   ├── customers
    │   └── calendar
    │
    └── Fact Data
        ├── order_items
        ├── order_returns
        └── order_shipments
```

The S3 landing location is exposed to Databricks through a Unity Catalog external location and accessed through the `raw_landing` volume.

```text
AWS S3
   │
   │ IAM Role + S3 Policy
   ▼
Databricks External Location
   │
   ▼
Unity Catalog Volume
   │
   ▼
/Volumes/ecommerce/raw/raw_landing/
```

### Dimension Data Ingestion

Dimension datasets are ingested using Spark batch processing.

An explicit schema is defined for the source CSV files to enforce the expected structure and data types during ingestion.

For example, the Brands dataset is read using a predefined schema:

```python
brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True)
])

df = (
    spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .schema(brand_schema)
    .csv(raw_data_path)
)
```

Additional ingestion metadata is captured to improve traceability:

- Source file path
- Source file modification timestamp
- Ingestion timestamp

The ingested data is then stored as Delta tables in the Bronze schema.

```text
S3 CSV Files
     │
     ▼
Spark Batch Read
     │
     ├── Explicit Schema
     └── Ingestion Metadata
     │
     ▼
Bronze Delta Tables
```

### Fact Data Ingestion

Fact datasets are ingested using **Databricks Auto Loader** with Structured Streaming.

For the historical load, Auto Loader is configured with `cloudFiles.includeExistingFiles` to process files already available in the S3 landing location.

The ingestion process includes:

- CSV file ingestion using `cloudFiles`
- Automatic schema inference
- Schema tracking
- Schema evolution using rescue mode
- Rescued data capture for unexpected fields
- Source file metadata
- Ingestion timestamp
- Checkpointing for processing state
- `availableNow` trigger for processing available files

Example flow:

```text
S3 Fact CSV Files
       │
       ▼
Databricks Auto Loader
       │
       ├── Schema Tracking
       ├── Rescue Mode
       ├── File Metadata
       └── Checkpointing
       │
       ▼
Bronze Delta Tables
```

The Bronze layer therefore provides the initial Delta representation of both dimensional and fact data before further cleansing and transformation in the Silver layer.

## Data Processing & Medallion Architecture

The data is processed using the **Medallion Architecture**, where data moves through three progressive layers:

```text
Bronze → Silver → Gold
```

The project uses separate processing flows for **dimension data** and **fact data**.

---

### 6.1 Bronze Layer — Raw Ingestion

The Bronze layer stores the initial ingested representation of the source data as Delta tables.

#### Dimension Processing

Dimension data is ingested using Spark batch processing with predefined schemas.

```text
S3 CSV Files
     │
     ▼
Spark Batch Read
     │
     ├── Schema Enforcement
     ├── Source File Metadata
     └── Ingestion Timestamp
     │
     ▼
Bronze Delta Tables
```

Example Bronze tables:

```text
brz_brands
brz_category
brz_customers
brz_products
brz_calendar
```

#### Fact Processing

Fact data is ingested using **Databricks Auto Loader** with Structured Streaming.

Auto Loader provides file-based ingestion with schema tracking, schema evolution, rescued data handling, and checkpointing.

```text
S3 Fact Files
     │
     ▼
Auto Loader
     │
     ├── Schema Tracking
     ├── Schema Evolution
     ├── Rescued Data
     ├── File Metadata
     └── Checkpointing
     │
     ▼
Bronze Delta Tables
```

Example:

```text
brz_order_items
```

---

### 6.2 Silver Layer — Data Cleansing & Transformation

The Silver layer contains cleansed, standardized, and validated data derived from the Bronze layer.

#### Dimension Transformations

Dimension data is read from Bronze Delta tables and transformed before being written to Silver.

Typical transformations implemented include:

- Duplicate removal
- Data standardization
- Column transformations
- Data type handling

For example, duplicate categories are removed based on `category_code`, and category codes are standardized using uppercase formatting.

```text
Bronze
   │
   ▼
Data Cleansing
   │
   ├── Remove Duplicates
   ├── Standardize Values
   └── Transform Columns
   │
   ▼
Silver
```

Example Silver tables:

```text
slv_brands
slv_category
slv_customers
slv_products
slv_calendar
```

#### Fact Transformations

Fact data is processed using Structured Streaming from the Bronze Delta table.

The fact transformation process includes:

- Duplicate handling using business keys
- Quantity standardization
- Numeric conversion
- Currency symbol removal
- Percentage conversion
- Coupon code standardization
- Channel standardization
- Processing timestamp generation

For `order_items`, duplicate records are identified using:

```text
order_id + item_seq
```

The transformed data is written to the Silver layer using a Delta `MERGE` operation.

```text
Bronze Delta
     │
     ▼
Structured Streaming
     │
     ├── Data Cleansing
     ├── Standardization
     ├── Deduplication
     └── Business Transformations
     │
     ▼
foreachBatch
     │
     ▼
Delta MERGE
     │
     ▼
Silver Delta Table
```

The Silver fact table is:

```text
slv_order_items
```

The `MERGE` logic updates existing records and inserts new records based on:

```text
order_id + item_seq
```

---

### 6.3 Gold Layer — Analytics & Dimensional Modeling

The Gold layer contains business-ready datasets designed for analytical consumption.

The project creates both **dimension tables** and **fact tables** in the Gold layer.

#### Gold Dimension Processing

Dimension data from the Silver layer is integrated to create analytical dimension tables.

For example, product data is enriched by joining product, brand, and category datasets.

```text
slv_products
      │
      ├──────────────┐
      ▼              ▼
slv_brands      slv_category
      │              │
      └──────┬───────┘
             ▼
      Gold Product Dimension
             │
             ▼
      gld_dim_products
```

The Gold dimension layer includes:

```text
gld_dim_customers
gld_dim_products
gld_dim_date
```

---

### 6.4 Gold Fact Processing

The Gold fact pipeline transforms the Silver order-item data into an analytics-ready fact table.

The processing includes the creation of business measures and analytical attributes such as:

- `gross_amount`
- `discount_amount`
- `sale_amount`
- `date_id`
- `coupon_flag`

The required business columns are selected and the resulting dataset is written to:

```text
gld_fact_order_items
```

The Gold fact processing also uses Delta-based batch processing and `MERGE` logic to update existing records and insert new records.

```text
slv_order_items
       │
       ▼
Business Transformations
       │
       ├── Gross Amount
       ├── Discount Amount
       ├── Sale Amount
       ├── Date Key
       └── Coupon Flag
       │
       ▼
gld_fact_order_items
```

---

### 6.5 Daily Gold Aggregation

A daily summary table is created from the Gold fact table for reporting and analytical use.

The pipeline identifies the latest available transaction date and processes a configurable historical window.

The data is aggregated by:

```text
date_id + currency
```

The summary calculates metrics including:

- Total Quantity
- Total Gross Amount
- Total Discount Amount
- Total Tax Amount
- Total Amount

```text
gld_fact_order_items
          │
          ▼
   Date Window Filter
          │
          ▼
    Group By Date
    + Currency
          │
          ▼
    Aggregated Metrics
          │
          ▼
gld_fact_daily_orders_summary
```

The summary table is maintained using Delta operations so that existing dates can be updated and new dates can be inserted.

---

### Medallion Processing Summary

```text
                         AWS S3
                           │
                           ▼
                    Raw Landing Area
                           │
             ┌─────────────┴─────────────┐
             │                           │
       Dimension Data               Fact Data
             │                           │
       Spark Batch                  Auto Loader
             │                           │
             ▼                           ▼
         ┌────────┐                  ┌────────┐
         │ Bronze │                  │ Bronze │
         └───┬────┘                  └───┬────┘
             │                           │
             ▼                           ▼
         ┌────────┐                  ┌────────┐
         │ Silver │                  │ Silver │
         └───┬────┘                  └───┬────┘
             │                           │
             ▼                           ▼
         ┌────────┐                  ┌──────────────┐
         │  Gold  │                  │ Gold Fact    │
         │  Dims  │                  │              │
         └────────┘                  └──────┬───────┘
                                            │
                                            ▼
                                  Daily Aggregation
                                            │
                                            ▼
                              gld_fact_daily_orders_summary
```
