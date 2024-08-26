# <span style="color:LightGreen">simple</span>-<span style="color:Crimson">E</span><span style="color:orange">T</span><span style="color:green">L</span>-<span style="color:#E6E6E6">PostgreSQL</span>
A simple ETL process example using PostgreSQL and PL/pgSQL. 

This PostgreSQL code outlines a simple ETL process for data staging, transformation, and normalization across three different datasets (staging_ins_a, staging_ins_b, staging_ins_c) and integrates them into a final normalized_claims table.

### 1. Table Creation (Staging Tables and Final Normalized Table)
- CREATE TABLE statements define three staging tables (staging_ins_a, staging_ins_b, staging_ins_c), representing data from different sources. Each table has fields related to insurance claims, including claim dates, service codes, and claim statuses.
- Another table, normalized_claims, is created to store the final integrated and transformed data.
### 2. Data Ingestion
- COPY statements are used to bulk load CSV data into the staging tables (staging_ins_a, staging_ins_b, and staging_ins_c).
- The data is read from CSV files and loaded into the respective tables using delimited input, which is efficient for bulk imports.
### 3. Data Validation (Statistical Summary)
- DO blocks with PL/pgSQL iterate over each column in the staging tables and calculate:
 - Null count (how many values are missing)
 - Unique count (distinct values)
 - Duplicate count (repeated values)
- These validation checks are implemented for data quality assurance using SQL INFORMATION_SCHEMA to dynamically query the schema and perform counts via EXECUTE statements.
### 4. Data Normalization & Insertion
- Data from the staging tables is transformed and inserted into the normalized_claims table through INSERT INTO SELECT queries.
- Each query pulls columns from the staging tables, applying the necessary transformations (e.g., renaming fields and adding metadata like payer_id, payer_name) and mapping them to the structure of normalized_claims.
- Each staging table corresponds to a different data provider (ins_A, ins_B, ins_C), which is reflected in the payer_id and payer_name fields in the normalized table.
### 5. Index Creation
- Several indexes are created on normalized_claims to optimize querying:
 - Single-column indexes on claim_date, claim_id, payer_id, cpt_code, and patient_id.
 - A composite index on (claim_date, payer_id) for improving query performance on combinations of claim date and payer.
