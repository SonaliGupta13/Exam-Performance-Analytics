## Databricks Notebooks – Medallion Architecture

This folder contains Databricks notebooks implementing the end-to-end ETL pipeline using the Medallion Architecture (Bronze, Silver, Gold).

### Notebooks Overview

- **01_bronze_ingestion.ipynb**  
  Ingests raw datasets from Excel/CSV sources into Databricks Bronze tables without transformation.

- **02_silver_cleaning.ipynb**  
  Cleans and validates Bronze data by handling null values, standardizing data types, and preparing data for analytics.

- **03_gold_logic.ipynb**  
  Applies business logic and transformations to generate analytics-ready Gold tables, including pass/fail status, merit rankings, KPIs, and performance trends.

Each notebook is executed sequentially as part of an automated Databricks Job Workflow.
