## Databricks Pipeline & Workflow

This folder contains screenshots of the **Databricks Job Workflow** used to automate the Exam Performance Analytics pipeline.

- **pipeline1.png**  
  Visual representation of the end-to-end Databricks job pipeline executing Bronze, Silver, and Gold notebooks sequentially.

- **pipeline2.png**  
  Timeline view showing execution order, duration, and successful completion of each pipeline stage.

### Pipeline Overview

The workflow automates the following steps:

1. Bronze layer ingestion of raw datasets  
2. Silver layer data cleaning and validation  
3. Gold layer business logic and analytics generation  

### Purpose

The automated pipeline ensures:
- Consistent and repeatable execution
- Reduced manual intervention
- Reliable data processing across layers

This workflow follows Databricks best practices for production-style ETL orchestration.
