/*
=============================================================================
Quality Checks
=============================================================================
what I have done here:
     This script performs various quality checks for data consistency, 
     accuracy standardization across the 'silver' schema. It includes checks
     for:
- Null or duplicate primary keys
- Unwanted spaces in string fields
- Data standardization and consistency
Using Npotes:
- Run these checks after data loading silver layer
- Insvestigate and resolve any discrepancies found during the checks
=============================================================================
*/

-- ============================================================================
-- PRINT'Scripts for  'silver.crm_cust_info table'
-- ============================================================================
-- we will go through all the table of broze layer 
-- we will analyse all the data and after analysing all the data 
-- we will clean all the data of nronze layer

select * 
from bronze.crm_cust_info;
go

select * 
from bronze.crm_sales_details;
go

-- a primary key must be unique and not null
-- aggreegate the primary to check wheather their are any duplicate of primary key or not
select
cst_id,
COUNT(*) as no_of_time_repeated
from bronze.crm_cust_info 
group by cst_ida
having count(*)>1;

-- JE HAVE ISSUE IN THIIS TABLE 
/* there re record where primary key is null
there re record where primary key has duplicate values
*/


-- check for unwanted spaces
-- Expectation: No results
-- since by just looking by data it is very difficult to find dat with unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname!=trim(cst_firstname);

-- now checking lastname of customer aslo
/* if the orinal values is not equal to the same value after trimming it 
means there are spaces!
*/
select cst_lastname 
from bronze.crm_cust_info
where cst_lastname!=TRIM(cst_lastname);

-- analysing the customer gender also
select cst_gndr
from bronze.crm_cust_info
where cst_gndr!=TRIM(cst_gndr);


-- Quality check of columns marital status and customer gndr both have abbreviated values so we will change those values  also 
-- Data Standardization & consistency
select DISTINCT cst_gndr
from bronze.crm_cust_info -- this will result only three genders

/*
Output as
M
F
Null

so we will update them as full name 

Male
Female
Unknown 
 
*/


-- ============================================================================
-- PRINT'Scripts for  'silver.crm_prd_info table'
-- ============================================================================

use DataWarehouse;
-- check for Nulls or Duplucates in Primary key
-- Expectation: No  result

select 
prd_id,
COUNT (*)
from bronze.crm_prd_info
Group by prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

-- output is showing there is no duplicte product id

-- now we checking product key


-- check for unwanted spaces
-- Expectation: No Results

select prd_nm
from bronze.crm_prd_info 
where prd_nm!=trim(prd_nm);

-- the already is fine there are no extra soaces in the words

-- checking quality of the numbers,check for nulls or negative Numbers
-- Expectation: No Reusult
select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost IS NULL;

-- out put we don't have any negative values but we have null
-- and we can haddle that by replacing the null with zeroes


-- now we checking product line now we given the abbreviation of prduct line
-- we will write it in the general form


-- checking the date columns
-- End Date must not be earlier then the start date
select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt; -- how can we correct it switch the stsrt date with end date

-- option one we can switch end date and start date
-- but not working logically correct
-- but correct logic is Derived End Date from the start date
-- End Date= Start Date of the 'NExt' Record-1;


-- ============================================================================
-- PRINT'Scripts for  'silver.crm_sales_details table'
-- ============================================================================

use DataWarehouse;
-- Check invalid dates 
-- Negative numbers or zeros cant't be cast to a date

-- NULLIF
-- return NULL if two given values are equal: otherwise, it returns the first expression
-- ih this scenario the length of the date must be 8
-- Also check the outliers by validating the boundries of the data range

select 
NULLIF(sls_order_dt,0)sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0
Or LEN (sls_order_dt)!=8
OR sls_order_dt>20500101
OR sls_order_dt<19000101

-- checking invalid date order
select * 
from silver.crm_sales_details
where sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt;


--- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales= Quantity*Price
-- >> Values must not be Null,Zero, or negative

select 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales !=sls_quantity*ABS(sls_price)
  Then sls_quantity * ABS(sls_price)
  else sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price<=0
     THEN sls_sales/NULLIF(sls_quantity,0)
else sls_price
END AS sls_price

FROM bronze.crm_sales_details
where sls_sales!=sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price is NULL
OR sls_sales <=0 OR sls_quantity <=0 OR  sls_price<=0 
Order by sls_sales,sls_quantity,sls_price

-- in these kind of situation 
-- Data Issues will be fixed direct in source system
-- Data Issues has to be fixed in data wahrehouse

--Few rules

/*
if sales is negative ,Zero or null derive it using Quantity and price 
if price is zero or null calculate it using Sales and Quantity
If price is negative, Convert it to a positive value+
*/


select * from silver.crm_sales_details;

-- ============================================================================
-- PRINT'Scripts for  'silver.erp_cust_az12 table'
-- ============================================================================


-- ============================================================================
-- PRINT'Scripts for  'silver.erp_loc_a101 table'
-- ============================================================================


-- ============================================================================
-- PRINT'Scripts for  'silver.erp_px_cat_g1v2 table'
-- ============================================================================
use DataWarehouse;
go

INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

select id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2
-- check for the unwanted spaces 
select * from bronze.erp_px_cat_g1v2
where cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);

-- Data standardization & Consistency
select 
distinct cat 
from bronze.erp_px_cat_g1v2;


select distinct 
subcat 
from bronze.erp_px_cat_g1v2;

select distinct 
maintenance from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2;
