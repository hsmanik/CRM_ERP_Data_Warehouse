-- Following the star schema for the gold layer

-- this is a dimension
-- first business object
create view gold_datawarehouse.dim_customers as 
select 
	row_number() over (order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for general info
    else coalesce(ca.gen, 'n/a')
    end as gender,
    ca.bdate as birth_date,
    ci.cst_create_date as create_date
from silver_datawarehouse.crm_cust_info as ci
left join silver_datawarehouse.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver_datawarehouse.erp_loc_a101 la
on ci.cst_key = la.cid;

-- chcking the date of the view

select distinct gender from gold_datawarehouse.dim_customers;

-- second business object
create view gold_datawarehouse.dim_products as 
select 
	row_number() over(order by pn.dwh_create_date, pn.prd_key) as product_key,
	pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt as start_date
from silver_datawarehouse.crm_prd_info pn
left join silver_datawarehouse.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

select * from gold_datawarehouse.dim_products;

-- creating the sales details
create view gold_datawarehouse.fact_sales as
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver_datawarehouse.crm_sales_details sd
left join gold_datawarehouse.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold_datawarehouse.dim_customers cu
on sd.sls_cust_id = cu.customer_id;

-- checking the integrity
select * 
from gold_datawarehouse.fact_sales as f
left join gold_datawarehouse.dim_customers c
on c.customer_key = f.customer_key
left join gold_datawarehouse.dim_products p
on f.product_key = p.product_number
where c.customer_key = null

