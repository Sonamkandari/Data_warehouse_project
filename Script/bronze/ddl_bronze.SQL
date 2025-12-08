/*
===============================================
DDL Script: Create Bronze Tables
===============================================

Script Purpose
    This script creates tables in the 'Bronze' schema, droping existing tables if they already exixt
    Run this script to re-definr the DDL structure of 'bronze' tables
===============================================    
  
*/
if object_id('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust_info;
go

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO

if object_id ('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info;
go
-- create SQL DDL scripts for all csv files in the crm and erp system
create table bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm  NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
GO

if object_id ('bronze.crm_sales_details','U') is not null
drop table bronze.crm_sales_details;
go

CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cus_id INT,
sls_order_id INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
GO

if OBJECT_ID('bronze.erp_loc_a101','u') is not null
drop table bronze.erp_loc_a101;

create table bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
GO


if object_id('bronze.erp_cust_az12') is not null
drop table bronze.erp_cust_az12;
go

create table bronze.erp_cust_az12(
cide NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);
go

if object_id('bronze.erp_px_cat_g1v2') is not null
drop table bronze.erp_px_cat_g1v2;
go
create table bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
)
go
