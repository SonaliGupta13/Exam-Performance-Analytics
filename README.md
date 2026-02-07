# Exam-Performance-Analytics
An end-to-end Databricks-based analytics platform that automates exam performance analysis using Medallion Architecture, PySpark, SQL Dashboards, and AI-powered natural language querying with Databricks Genie.

### Project Overview
Educational institutions often rely on manual Excel-based reporting to analyze exam results, which is time-consuming, error-prone, and lacks advanced insights.
This project builds a scalable analytics pipeline to:

- Ingest raw exam data
- Clean and transform datasets
- Apply business rules (cutoffs, pass/fail, ranking)
- Generate KPIs and dashboards
- Enable AI-driven insights using natural language queries

### Tools Used
- Databricks
- PySpark
- SQL Analytics
- Databricks Genie AI
- Microsoft Excel (Raw data source)

### Tech Stack
- Data Engineering
- ETL Pipelines
- Medallion Architecture
- Big Data Analytics
- AI Integration
- Dashboarding & Visualization

### Architecture Overview (Medallion Framework)
The solution follows Databricks Medallion Architecture for reliable and scalable data processing.

#### Bronze Layer
- Raw data ingestion
- No transformations

#### Silver Layer
- Data cleaning & validation
- Null handling
- Data type standardization

#### Gold Layer
- Business logic & analytics
- KPIs, rankings, trends
- Dashboard-ready tables

### Databricks Pipeline Workflow
The project uses an automated Databricks Job Workflow:

**01_bronze_ingestion  →  02_silver_cleaning  →  03_gold_logic**

#### Execution Summary:

- Bronze Ingestion: ~1 minute
- Silver Cleaning: ~11 seconds
- Gold Business Logic: ~27 seconds

✔ Fully automated
✔ Sequential execution
✔ Reliable & repeatable

### AI Integration – Databricks Genie
Databricks Genie enables natural language analytics on exam data.

#### Example Genie Questions:

- How many students are there in each city?
- Who is the top student according to marks?
- Who is the lowest performer with attendance status present?
- Which category performs best?

Genie automatically converts questions into optimized SQL and generates visual insights.

### Dashboard & KPIs
The Exam Performance Dashboard is built using Databricks SQL Analytics.

#### Key KPIs:

- Total Present Students
- Overall Attendance Percentage
- Overall Pass Percentage
- Overall Average Marks

#### Visual Insights:

- Pass % by City
- Pass % by Category
- Average Marks by Category
- Attempt-wise Improvement Trend
- Top Students
- Low Performers

### Key Insights Generated

- **Top Student Overall:** Meera_985 with 95 marks

- **Lowest Performer (Present):** Neha_19674 with 3 marks

- **Underperforming Students:** 1,331 present students scored below 50

- **Best Performing Category:** EWS (highest average marks)

- **Highest Student Count City:** Lucknow
