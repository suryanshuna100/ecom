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
