## CRM and ERP Data Warehouse Project

This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

## Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![image](https://github.com/hsmanik/CRM_ERP_Data_Warehouse/blob/main/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics

## [Dataset](https://github.com/hsmanik/CRM_ERP_Data_Warehouse/tree/main/datasets)
