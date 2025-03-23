-- creating database for different layers of ETL
CREATE DATABASE bronze_datawarehouse;
CREATE DATABASE silver_datawarehouse;
CREATE DATABASE gold_datawarehouse;

-- creating tables for different layers
USE bronze_datawarehouse;
drop table if exists bronze_datawarehouse.crm_prd_info;
CREATE TABLE bronze_datawarehouse.crm_prd_info (
	prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(10),
    prd_start_dt date,
    prd_end_dt date
);

drop table if exists bronze_datawarehouse.crm_sales_details;
create table bronze_datawarehouse.crm_sales_details (
	sls_ord_num	varchar(50),
    sls_prd_key	varchar(50),
    sls_cust_id	int,
    sls_order_dt int,	
    sls_ship_dt	int,
    sls_due_dt	int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);
select count(*) from bronze_datawarehouse.crm_sales_details;

drop table if exists bronze_datawarehouse.crm_cust_info;
create table bronze_datawarehouse.crm_cust_info (
	cst_id	int,
    cst_key	varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status	varchar(50),
    cst_gndr varchar(50),
    cst_create_date date
);

drop table if exists bronze_datawarehouse.erp_cust_az12;
create table bronze_datawarehouse.erp_cust_az12 (
	cid varchar(50),
    bdate date,
    gen varchar(30)
);

drop table if exists bronze_datawarehouse.erp_loc_a101;
create table erp_loc_a101 (
	cid	varchar(50),
    cntry varchar(50)
);

drop table if exists bronze_datawarehouse.erp_px_cat_g1v2;
create table bronze_datawarehouse.erp_px_cat_g1v2 (
	id varchar(50),
    cat varchar(50),	
    subcat varchar(50),
    maintenance varchar(50)
);

select * from crm_cust_info;

-- Building the Silver Layer tables
-- To ensure that the dwh_create_date column automatically records the timestamp when a new row is inserted
USE silver_datawarehouse;
drop table if exists silver_datawarehouse.crm_prd_info;
CREATE TABLE silver_datawarehouse.crm_prd_info (
	prd_id int,
    prd_key varchar(50),
    cat_id varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(35),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime default current_timestamp
);

drop table if exists silver_datawarehouse.crm_sales_details;
create table silver_datawarehouse.crm_sales_details (
	sls_ord_num	varchar(50),
    sls_prd_key	varchar(50),
    sls_cust_id	int,
    sls_order_dt date,	
    sls_ship_dt	date,
    sls_due_dt	date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date datetime default current_timestamp
);
select * from silver_datawarehouse.crm_sales_details;

drop table if exists silver_datawarehouse.crm_cust_info;
create table silver_datawarehouse.crm_cust_info (
	cst_id	int,
    cst_key	varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status	varchar(50),
    cst_gndr varchar(50),
    cst_create_date date,
    dwh_create_date datetime default current_timestamp
);

drop table if exists silver_datawarehouse.erp_cust_az12;
create table silver_datawarehouse.erp_cust_az12 (
	cid varchar(50),
    bdate date,
    gen varchar(30),
    dwh_create_date datetime default current_timestamp
);
select * from silver_datawarehouse.erp_cust_az12;

drop table if exists silver_datawarehouse.erp_loc_a101;
create table silver_datawarehouse.erp_loc_a101 (
	cid	varchar(50),
    cntry varchar(50),
    dwh_create_date datetime default current_timestamp
);
select * from silver_datawarehouse.erp_loc_a101;

drop table if exists silver_datawarehouse.erp_px_cat_g1v2;
create table erp_px_cat_g1v2 (
	id varchar(50),
    cat varchar(50),	
    subcat varchar(50),
    maintenance varchar(50),
    dwh_create_date datetime default current_timestamp
);
select * from silver_datawarehouse.erp_px_cat_g1v2;

-- building the tables for gold layer
USE gold_datawarehouse;
drop table if exists gold_datawarehouse.crm_prd_info;
CREATE TABLE gold_datawarehouse.crm_prd_info (
	prd_id int,
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(10),
    prd_start_dt date,
    prd_end_dt date
);

drop table if exists gold_datawarehouse.crm_sales_details;
create table gold_datawarehouse.crm_sales_details (
	sls_ord_num	varchar(50),
    sls_prd_key	varchar(50),
    sls_cust_id	int,
    sls_order_dt int,	
    sls_ship_dt	int,
    sls_due_dt	int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

drop table if exists gold_datawarehouse.crm_cust_info;
create table gold_datawarehouse.crm_cust_info (
	cst_id	int,
    cst_key	varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status	varchar(50),
    cst_gndr varchar(50),
    cst_create_date date
);

drop table if exists gold_datawarehouse.erp_cust_az12;
create table erp_cust_az12 (
	cid varchar(50),
    bdate date,
    gen varchar(30)
);

drop table if exists gold_datawarehouse.erp_loc_a101;
create table erp_loc_a101 (
	cid	varchar(50),
    cntry varchar(50)
);

drop table if exists gold_datawarehouse.erp_px_cat_g1v2;
create table erp_px_cat_g1v2 (
	id varchar(50),
    cat varchar(50),	
    subcat varchar(50),
    maintenance varchar(50)
);
