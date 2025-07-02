# End-to-End-Resume-Data-Pipeline

An end-to-end resume ingestion and analytics pipeline built on **Databricks**, using **Apache Spark**, **Delta Lake**, and **PySpark**. This project simulates real-world resume data ingestion, parsing, transformation, dimension modeling (star schema), and reporting using SQL-based analytics.

---

## Overview

This pipeline simulates the full lifecycle of resume processing — from ingestion of raw resume data to structured parsing, applying Slowly Changing Dimension Type 2 (SCD2) logic, and generating analytical insights such as:

- Top skills across categories
- Education level distribution
- Parsing success rate
- Usable vs unusable resumes
- Resume trends over time

---

## Architecture

```text
 ┌──────────────────────────────┐
 │ 00a_generate_resume_upload_db│  <-- Simulates raw resumes
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │00b_load_master_source        │  <-- Loads to Delta Lake (resume_master_source)
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │01_simulate_daily_ingestion   │  <-- Pulls new data based on processing date
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │02_parse_resumes              │  <-- Extracts name, email, skills, etc.
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │03_transform_and_scd2         │  <-- Cleans data, applies SCD2, builds dimensions
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │04_load_fact_tables           │  <-- Creates fact tables with joins
 └────────────┬─────────────────┘
              ▼
 ┌──────────────────────────────┐
 │05_reporting_queries          │  <-- SQL dashboards and KPIs
 └──────────────────────────────┘
```

---

## Project Structure

```
notebooks/
├── 00a_generate_resume_upload_db.py        # Simulates fake resume uploads
├── 00b_load_master_source.py               # Loads into resume_master_source (Delta table)
├── 01_simulate_daily_ingestion.py          # Pulls resumes for a given processing date
├── 02_parse_resumes.py                     # Parses raw resume text (skills, emails, etc.)
├── 03_transform_and_scd2.py                # Cleans data and applies SCD Type 2 logic
├── 04_load_fact_tables.py                  # Builds bridge and fact tables for analytics
├── 05_reporting_queries.sql                # Contains all reporting SQL queries
```

---

## Tech Stack

| Tool/Framework    | Purpose                          |
|-------------------|----------------------------------|
| **Databricks**    | Unified data engineering         |
| **Apache Spark**  | Distributed data processing      |
| **Delta Lake**    | ACID-compliant table storage     |
| **PySpark**       | Data transformation & SCD logic  |
| **SQL**           | Reporting & analytics            |
| **Jinja (optional)** | For SQL templating (if used)   |

---

## Key Features

- Simulates resume ingestion with timestamps
- Parses resumes to extract structured fields
- Handles duplicate candidates using latest `upload_date`
- Implements SCD Type 2 on `dim_candidate`
- Builds star-schema-compliant dimension and fact tables
-  Supports SQL analytics: top skills, trends, resume quality

---

## Sample Queries

- Top 10 most common skills
- Count of usable resumes by category
- Education level distribution
- Candidates with no skills detected
- Parsing success rate by day
- Drill-down: candidate + skills + education

---

## Reporting Highlights

The notebook `05_reporting_queries.sql` includes all insights such as:

```sql
-- Example: Top 5 skills in each category
WITH ranked AS (
  SELECT c.category_id, s.skill, COUNT(*) AS skill_count,
         ROW_NUMBER() OVER (PARTITION BY c.category_id ORDER BY COUNT(*) DESC) AS rank
  FROM workspace.resume_project.bridge_candidate_skill b
  JOIN workspace.resume_project.dim_candidate c ON b.candidate_id = c.candidate_id
  JOIN workspace.resume_project.dim_skill s ON b.skill_id = s.skill_id
  WHERE c.is_current = true
  GROUP BY c.category_id, s.skill
)
SELECT * FROM ranked WHERE rank <= 5;
```

---

## Testing & Validation

- Each notebook has `.show()` calls to visually validate transformations.
- Manual edge-case tested: duplicate candidates, missing fields, re-processing batches.
- Schema validations done before each overwrite or merge.

---

## Scheduling

All notebooks can be orchestrated using a **Databricks Job Scheduler**, e.g.:

```python
# scheduler_notebook.py (example)
dbutils.notebook.run("00a_generate_resume_upload_db", 60)
dbutils.notebook.run("00b_load_master_source", 60)
dbutils.notebook.run("01_simulate_daily_ingestion", 60)
dbutils.notebook.run("02_parse_resumes", 60)
dbutils.notebook.run("03_transform_and_scd2", 60)
dbutils.notebook.run("04_load_fact_tables", 60)
dbutils.notebook.run("05_reporting_queries", 60)
```

---

## Future Improvements

- Add ML-based skill classification
- Include resume similarity matching
- Integrate with real-world resume parsers (e.g., spaCy, PDFMiner)
- Build a dashboard using Power BI or Tableau

---

## Author

**Ujjawal Gusain**  
Data Engineer | Resume Intelligence Systems  
[LinkedIn](https://www.linkedin.com/in/ujjawalgusain31)

---


