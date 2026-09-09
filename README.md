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
