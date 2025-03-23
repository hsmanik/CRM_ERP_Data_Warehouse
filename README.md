## CRM and ERP Data Warehouse Project

This ETL (Extract, Transform, Load) project is designed to ingest, process, and store data from ERP and CRM CSV files using the Medallion Architecture and Python-MySQL. The objective is to ensure structured, clean, and enriched data is stored efficiently in a database for further analysis and reporting

## Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![image](https://github.com/hsmanik/CRM_ERP_Data_Warehouse/blob/main/data_architecture.png)

1. **Bronze Layer (Raw Data Storage)**
- CSV files from ERP and CRM systems are ingested into a MySQL database.
- Data is stored in its raw format with minimal preprocessing.

2. **Silver Layer (Cleaned & Processed Data)**
- Data cleansing, formatting, and transformation using MySQL.
- Handling missing values, data type conversions, and deduplication.
- Structuring data for easier querying and analysis.

3. **Gold Layer (Optimized & Analytical Data)**
- Aggregated and enriched data ready for reporting and business intelligence.
- Creation of key metrics, calculated fields, and indexing for performance optimization.

## [Dataset](https://github.com/hsmanik/CRM_ERP_Data_Warehouse/tree/main/datasets)
