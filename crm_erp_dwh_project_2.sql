-- understanding the data in the bronze warehouse

select * from bronze_datawarehouse.crm_cust_info;
select * from bronze_datawarehouse.crm_prd_info;
select * from bronze_datawarehouse.crm_sales_details;
select * from bronze_datawarehouse.erp_cust_az12;
select * from bronze_datawarehouse.erp_loc_a101;
select * from bronze_datawarehouse.erp_px_cat_g1v2;


-- Data cleaning and quality


-- check for unwanted spaces
select cst_firstname
from bronze_datawarehouse.crm_cust_info
where cst_firstname != trim(cst_firstname);


-- check for Nulls or duplicate in primary key
select cst_id, count(*) 
from bronze_datawarehouse.crm_cust_info
group by 1
having count(*) > 1 or cst_id is null;

with rank_table as(
select *, row_number() over(partition by cst_id order by cst_create_date desc) as ranking
from bronze_datawarehouse.crm_cust_info
)
select * from rank_table where ranking = 1;

-- checking the consistency for low cardinality columns
select *, case  
	when cst_gndr = 'M' then 'Male'
    when cst_gndr = 'F' then 'Female'
    end
from bronze_datawarehouse.crm_cust_info;


-- master query and inserting into the silver layer
INSERT INTO silver_datawarehouse.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr,cst_create_date)
with rank_table as(
select cst_id, 
	cst_key, 
	trim(cst_firstname) as cst_firstname, 
    trim(cst_lastname) as cst_lastname, 
	case 
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'n/a'
        end as cst_marital_status, 
	case 
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'n/a'
    end as cst_gndr,
    cst_create_date,  
    row_number() over(partition by cst_id order by cst_create_date desc) as ranking
from bronze_datawarehouse.crm_cust_info
)
SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date 
from rank_table where ranking = 1;


use silver_datawarehouse;
select cst_id, count(*) 
from silver_datawarehouse.crm_cust_info
group by 1
having count(*) > 1 or cst_id is null;

select * from crm_cust_info;

select cst_firstname
from bronze_datawarehouse.crm_cust_info
where cst_firstname != trim(cst_firstname);


-- cleaning and loading crm_prd_info and crm_sales_details into silver layer

select * from bronze_datawarehouse.crm_prd_info;

select prd_id, count(*)
from bronze_datawarehouse.crm_prd_info
group by 1
having count(*) > 1 or prd_id is null;


-- Master Query for crm_prd_info inserting into the silver layer

insert into silver_datawarehouse.crm_prd_info (prd_id, prd_key, cat_id ,prd_nm, prd_cost,prd_line, prd_start_dt, prd_end_dt)
select prd_id, 
	substring(prd_key,7, length(prd_key)) as prd_key, 
	replace(substring(prd_key,1,5),'-','_')  as cat_id, 
	prd_nm, 
    ifnull(prd_cost,0) as prd_cost , 
    case 
		when upper(trim(prd_line)) = 'R' then 'Roads'
        when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
	end as prd_line, 
    prd_start_dt, 
    lead(prd_start_dt) over(partition by prd_key order by prd_start_dt ) as prd_end_dt
from bronze_datawarehouse.crm_prd_info;


-- crm_sales_details

-- master query for inserting data into the silver datawarehouse
insert into silver_datawarehouse.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
select sls_ord_num, 
	sls_prd_key, 
    sls_cust_id, 
	case 
    when sls_order_dt = 0 or length(sls_order_dt) != 8 then null
    else cast(sls_order_dt as date) 
    end as sls_order_dt, 
    case 
    when sls_ship_dt = 0 or length(sls_ship_dt) != 8 then null
    else cast(sls_ship_dt as date) 
    end as sls_ship_dt, 
    case 
    when sls_due_dt = 0 or length(sls_due_dt) != 8 then null
    else cast(sls_due_dt as date) 
    end as sls_due_dt,
    case 
		when sls_sales != sls_quantity * sls_price then sls_quantity * abs(sls_price)
		when sls_sales is null or sls_sales <= 0 then sls_quantity * abs(sls_price)
        else sls_sales
	end as sls_sales, 
    sls_quantity,
    case 
		when sls_price is null then format(sls_sales / nullif(sls_quantity,0),0 )
        when sls_price <= 0 then abs(sls_price)
        else sls_price
	end as sls_price
from bronze_datawarehouse.crm_sales_details;

-- chacking for invalid date orders
select * from silver_datawarehouse.crm_sales_details where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- checking sales = quantity * sls_price
-- values must not be null, zero or negative.

select distinct sls_sales as old_sls_sales , sls_quantity, sls_price as old_sls_sales,
	case 
		when sls_sales != sls_quantity * sls_price then sls_quantity * abs(sls_price)
		when sls_sales is null or sls_sales <= 0 then sls_quantity * abs(sls_price)
        else sls_sales
	end as sls_sales,
    case 
		when sls_price is null then format(sls_sales / nullif(sls_quantity,0),0 )
        when sls_price <= 0 then abs(sls_price)
        else sls_price
	end as sls_price
from silver_datawarehouse.crm_sales_details
where sls_sales != sls_quantity * sls_price or 
sls_sales is null or sls_quantity is null or sls_price is null or
sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by 1,2,3;



-- ERP cust_az12

select 
	case
		when cid like 'NAS%' then substring(cid, 4, length(cid))
        else cid
	end as cid,
	bdate, gen 
from bronze_datawarehouse.erp_cust_az12
where case
		when cid like 'NAS%' then substring(cid, 4, length(cid))
        else cid
	end  not in (select distinct cst_key from silver_datawarehouse.crm_cust_info);


-- master Query for insertin into silver_datawarehouse
insert into silver_datawarehouse.erp_cust_az12 (cid,bdate,gen)
select 
	case
		when cid like 'NAS%' then substring(cid, 4, length(cid))
        else cid
	end as cid,
	case 
		when bdate > curdate() then null
		else bdate
	end as bdate,
    case 
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        when upper(trim(gen)) in ('M','MALE') then 'Male'
        else 'n/a'
	end as gen
from bronze_datawarehouse.erp_cust_az12;


-- working on erp_loc_a101 table

insert into silver_datawarehouse.erp_loc_a101 (cid, cntry)
select replace(cid, '-','') as cid , 
	case 
		when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'n/a'
        else trim(cntry)
	end as cntry
from bronze_datawarehouse.erp_loc_a101;


-- working on erp_px_cat_g1v2

insert into silver_datawarehouse.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
select id, cat, subcat, maintenance from bronze_datawarehouse.erp_px_cat_g1v2;