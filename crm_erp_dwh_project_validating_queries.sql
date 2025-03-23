select nullif(sls_due_dt, 0) as sls_due_dt from crm_sales_details 
where sls_due_dt <= 0 or length(sls_due_dt) != 8 or sls_due_dt > 20500101 or sls_due_dt < 19000101;

select sls_sales, sls_quantity, sls_price from silver_datawarehouse.crm_sales_details
where --  sls_sales != sls_quantity * sls_price or 
sls_sales is null or sls_quantity is null or sls_price is null or
sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0;

select distinct bdate from silver_datawarehouse.erp_cust_az12 
where bdate < '1924-01-01' or bdate > curdate();

select distinct gen, 
	case 
		when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
        when upper(trim(gen)) in ('M','MALE') then 'Male'
        else 'n/a'
	end as gen2
from silver_datawarehouse.erp_cust_az12;


select replace(cid, '-','') AS cid, cntry 
from bronze_datawarehouse.erp_loc_a101 where replace(cid, '-','') not in (select cst_key from silver_datawarehouse.crm_cust_info);

select distinct cntry
from silver_datawarehouse.erp_loc_a101
order by cntry;

-- unwanted spaces
select * from bronze_datawarehouse.erp_px_cat_g1v2
where maintenance != trim(maintenance);

select distinct
	ci.cst_gndr,
    ca.gen,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for general info
    else coalesce(ca.gen, 'n/a')
    end as new_gen
from silver_datawarehouse.crm_cust_info as ci
left join silver_datawarehouse.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver_datawarehouse.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2;  

delete from silver_datawarehouse.crm_cust_info where  cst_key = 'SF566';
	
select * from silver_datawarehouse.crm_cust_info where cst_key = 'SF566'