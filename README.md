# Data_warehouse_project 

- **Welcome to the Data Warehouse and Analytical Project repository.
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry-based practices in the data engineering and analytics.** 
---
- The first step in my work is to create a clear project plan.
- For this purpose, I am using Notion, a tool that helps organize plans, ideas, and documentation in one place.
- A well-structured project plan is essential for executing a successful project.
- Since I am currently in the initial phase of my project, I am using Notion to create a basic project roadmap.
- Using Notion, I will outline the main phases, milestones, and stages of the project.
---
  # What we’re going to do in this project are
  **project requirement** Building the data warehouse (data engineering).
 - **Objective** The objective of the project is to develop a modern data warehouse using SQL serevr to consolidate sales data, enabling analytical reports and informed decision making.
## What are the **specifications** of the project?
1. **Data sources** - Import data from the source systems, ERP and CRM provided  as CSV file.
2. **Data quality** - Cleans and resolves data quality issues prior to analysis.
3. **Integration** - Combine both sources into a single user-friendly data model designed for analytical queries.
4. **Scope** - Focus on the latest dataset only (historic data). Authorization of the data is not required.
5. **Documentation** - Provide a clean documentation model to support both businesses and stakeholders' analytical requirements.
---
<img width="754" height="541" alt="image" src="https://github.com/user-attachments/assets/84564431-9d9f-4e42-ad6a-e62296514548" />

---
- Building Data Architecture
<img width="1099" height="445" alt="image" src="https://github.com/user-attachments/assets/4e3d4cf0-5056-4047-abe6-5a2536e94527" />
---

* Design the layers of data Architecture
---
 <img width="742" height="528" alt="image" src="https://github.com/user-attachments/assets/06807a7f-32b5-4706-abbf-846eba1fcc8d" />
 
---
## **Seperation of concerns**
---

 <img width="764" height="495" alt="image" src="https://github.com/user-attachments/assets/e085e083-744d-460d-83ee-970f353ecb41" />
 
---
- The component of your architecture must not be duplicated. So, we cannot have two parts doing the same thing.
- The idea here is not to mix the two parts. 
- if you are a good data architect you should never combine diffrent layers of
---

<img width="801" height="523" alt="image" src="https://github.com/user-attachments/assets/16a9523d-239d-4e1a-a043-d2573e3386b6" />

---
- keep SOC  [sepration of concerns] -> A good data architect follows this concept
--- 

# Using Draw.io to desining  and draw the Data Architecture 
---
  <img width="861" height="417" alt="image" src="https://github.com/user-attachments/assets/e68e0217-c235-4c12-8cbe-51770760768e" />
  
---

 - right now all the conatiner are empty so we need to add content in these containers
 - What are different types of source systems which are connected to a data structure architecture?
   
---
<img width="191" height="421" alt="image" src="https://github.com/user-attachments/assets/04ef39ef-5458-4cdc-81d8-7f63a23bdd59" />

---
* This way, we had made in the data architecture clear that what are the sources of the data.
- Designed the sources of the High level Architecture
---  
<img width="187" height="430" alt="image" src="https://github.com/user-attachments/assets/01a88d82-066a-45cb-acd9-f4f880d41b15" />

---
- Next thing we're going to do is design what will be the content of the bronze, silver, and gold layers.
--- 
<img width="1007" height="749" alt="image" src="https://github.com/user-attachments/assets/07f5eb7a-5457-4263-a7c6-701cbef19ceb" />

---
- One more resource for icons we had used that is flaticons from this we can any specific and new special icon in our drawing board
---
# General Principles 
# Defining Naming Conventions
* Set of Rules Or Guidelines for naming anything in the project
  - Database
  - Schema
  - Tables
  - Store Procedures
* For this project we gonna use nnake_case naming convension
---
  <img width="669" height="390" alt="image" src="https://github.com/user-attachments/assets/5441c054-d6df-428a-a79f-68a5d46861f5" />
  
---
# General Principles

- Naming Conventions: Use snake_case, with lowercase letters and underscores (_) to separate words.
- Language: Use English for all names.
- Avoid Reserved Words: Do not use SQL reserved words as object names.



# Table Naming Conventions

## Bronze Rules

All names must start with the source system name, and table names must match their original names without renaming.

- <sourcesystem>_<entity>
  - <sourcesystem>: Name of the source system (e.g., crm, erp).
  - <entity>: Exact table name from the source system.

  Example: crm_customer_info → Customer information from the CRM system.



## Silver Rules

All names must start with the source system name, and table names must match their original names without renaming.

- <sourcesystem>_<entity>
  - <sourcesystem>: Name of the source system (e.g., crm, erp).
  - <entity>: Exact table name from the source system.

  Example: crm_customer_info → Customer information from the CRM system.



## Gold Rules

All names must use meaningful, business-aligned names for tables, starting with the category prefix.

- <category>_<entity>
  - <category>: Describes the role of the table, such as dim (dimension) or fact (fact table).
  - <entity>: Descriptive name of the table, aligned with the business domain (e.g., customers, products, sales).

  Examples:
  - dim_customers → Dimension table for customer data.
  - fact_sales → Fact table containing sales transactions.



# Glossary of Category Patterns

| Pattern | Meaning           | Example(s)                      |
|---------|-------------------|---------------------------------|
| dim_    | Dimension table   | dim_customer, dim_product       |
| fact_   | Fact table        | fact_sales                      |
| agg_    | Aggregated table  | agg_customers, agg_sales_monthly |



# Column Naming Conventions

## Surrogate Keys

All primary keys in dimension tables must use the suffix _key.

- <table_name>_key
  - <table_name>: Refers to the name of the table or entity the key belongs to.
  - _key: A suffix indicating that this column is a surrogate key.

  Example: customer_key → Surrogate key in the dim_customers table.



## Technical Columns

All technical columns must start with the prefix dwh_, followed by a descriptive name indicating the column’s purpose.

- dwh_<column_name>
  - dwh: Prefix exclusively for system-generated metadata.
  - <column_name>: Descriptive name indicating the column’s purpose.

  Example: dwh_load_date → System-generated column used to store the date when the record was loaded.



# Stored Procedure

All stored procedures used for loading data must follow the naming pattern:

- load_<layer>
  - <layer>: Represents the layer being loaded, such as bronze, silver, or gold.

  Examples:
  - load_bronze → Stored procedure for loading data into the Bronze layer.
  - load_silver → Stored procedure for loading data into the Silver layer.
 
---
## Project Initialization
* Create Database & Schemas
---  
  <img width="720" height="525" alt="image" src="https://github.com/user-attachments/assets/a0741990-5dbd-434b-ad36-f3f3bc2847f4" />
---

# Now start with Bronze layer
- Step 1: Analyse the source system
- understand the source system
- After understanding the source system start coding [Data ingestion]
- Data validation -> data Completeness and schema check -> is data is placed to its right position or not
- last step is documentation of our work
---
<img width="935" height="207" alt="image" src="https://github.com/user-attachments/assets/68f9934b-ec17-4e5f-9b42-99334355407c" />

---
## Build source Systems 
- Analyse Source Systems
---
  <img width="763" height="470" alt="image" src="https://github.com/user-attachments/assets/6e022eb6-8e4d-4b20-8923-0c77644a118c" />

---
- started Building Bronge layer
- Create DDL for Tables
-  here are some specification of bronze layer
 
---
  <img width="405" height="523" alt="image" src="https://github.com/user-attachments/assets/b0cb92d1-48ac-4e93-bcfc-e3caeab9a614" />
    
---
- DDl -> Data Defination languages define the  structure of database yables
---
- now we are wrinting SQL  queries so that we can connect source system to our source system

 * We write SQL queries to integrate data from the source system into SQL Server.

If the source data is in an Excel or CSV file (e.g., customer details), we create a table in SQL Server that matches the structure of the file: same columns, correct data types, and a suitable table name.

Once the table is created, we can load/import the data into SQL Server without errors.
---
- created data base and shemas of all the tree layers
<img width="1235" height="576" alt="image" src="https://github.com/user-attachments/assets/f65ac2fe-38cf-4964-a816-841e751f921f" />

 ---
 - Create SQL DDL script for all CSV files. 

  ```
/*

============================================
Create database and schemas
============================================

Script purpose:
This script creates a new database named 'dataWarehouse' after checking if it already exists.
If the database exists, it drops and recreated. Additionally, the script sets up the three schemas
within the database: bronze, silver, and gold.

Warning:
Running this script will drop the entire data warehouse database if it exists. 
All data in the database will permanently be deleted. Proceed with caution 
and ensure you have proper backups before running this script 

*/


USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

if object_id('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust_info;
go

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
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
```
# Develop SQL load script
- and the method used for loading the data from source file to data base is **bulk insert**

---

<img width="1246" height="857" alt="image" src="https://github.com/user-attachments/assets/4be63e87-6278-47f2-88fc-99220510ae14" />

---
- In the similar way i had connected all the data resources and csv files to this layer and improted all the data
---
```
-- creating stored procedures
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN 
    BEGIN  TRY

        PRINT'=========================================';
        PRINT'Loading Bronze Layer';
        PRINT'=========================================';



        PRINT'-----------------------------------------';
        PRINT'Loading CRM Tables';
        PRINT'-----------------------------------------';

        PRINT'>> Truncating Table:bronze.crm_cust_info '
        Truncate table bronze.crm_cust_info;
        -- Write SQL BULK insert to load all CSV Files
        PRINT'>>Inserting Data Into:bronze.crm_cust_info '
        BULK INSERT bronze.crm_cust_info
        from 'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
         firstrow=2,
         fieldterminator=',',
         tablock
        );

        PRINT'>> Truncating Table:bronze.crm_prd_info '
        Truncate table bronze.crm_prd_info;
        PRINT'>>Inserting Data Into:bronze.crm_prd_info '
        BULK INSERT bronze.crm_prd_info
        from 'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR=',',
            TABLOCK
        );


        PRINT'>> Truncating Table:crm_sales_details '
        Truncate table bronze.crm_sales_details;
        PRINT'>>Inserting Data Into:crm_sales_details '
        bulk insert bronze.crm_sales_details
        from 'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR=',',
            TABLOCK
        );


        PRINT'-----------------------------------------';
        PRINT'Loading ERP Tables';
        PRINT'-----------------------------------------';

        -- load the data of erp also
        PRINT'>> Truncating Table:bronze.erp_cust_az12 '
        TRUNCATE table bronze.erp_cust_az12;
        PRINT'>>Inserting Data Into:bronze.erp_cust_az12 '
        bulk insert bronze.erp_cust_az12
        from 'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH(
            FIRSTROW =2,
            FIELDTERMINATOR=',',
            TABLOCK
        );

        PRINT'>> Truncating Table:bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT'>>Inserting Data Into:bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        from'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        with(
           FIRSTROW= 2,
           FIELDTERMINATOR=',',
           TABLOCK
        );

        PRINT'>> Truncating Table:bronze.erp_px_cat_g1v2 '
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT'>>Inserting Data Into:bronze.erp_px_cat_g1v2 '
        BULK INSERT bronze.erp_px_cat_g1v2
        from'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        with(
           FIRSTROW= 2,
           FIELDTERMINATOR=',',
           TABLOCK
        );

        /*
        select * from bronze.erp_cust_az12;
        select count(*) from bronze.erp_cust_az12;
        select count(*) from bronze.crm_sales_details;
        select count(*) from bronze.crm_cust_info;
        select count(*) from bronze.erp_loc_a101;
        select count(*) from bronze.erp_px_cat_g1v2;

        */
    END TRY
    BEGIN CATCH 
    END CATCH
END


```
- Done with loading data from source file to data base
---
## 
<img width="594" height="277" alt="image" src="https://github.com/user-attachments/assets/0a5b4bbc-f8b2-4b26-a4a2-2144e0cfe56e" />
---
### NOTE: save frquantly used SQL code in stored procedures in database
<img width="1136" height="682" alt="image" src="https://github.com/user-attachments/assets/83488c83-c2f4-42a1-adc5-bd9c52f1cc92" />

---
- Note by using this sql are bale to execute all the csv files directly
- sql runs the try block and if it fails it runs the CATCH block to handle the Error
```
EXEC.bronze.load_bronze
-- Add Prints to track execution, debug issues, and understand its flow

```
---

<img width="364" height="484" alt="image" src="https://github.com/user-attachments/assets/c84753f5-f490-4a6f-9e39-04dada2367ba" />

---
## ETL PROCESS
- Extract , Transmission ,Load Process
- Note: **Bottle Neck**:A bottleneck is a point in a process where the flow of work slows down because that step cannot handle the workload coming into it.
- 📌 Simple Definition
-A bottleneck is the slowest or most limiting part of a system that reduces the overall speed or performance of the entire process.
- 📌 Why is it called a bottleneck?
- Think of a water bottle:
- The bottle is wide → can hold a lot of water
- But the neck is narrow → only a small amount can flow out at a time
- Similarly, in any workflow, the narrowest (slowest) part controls how fast the entire process can move.
---
<img width="684" height="71" alt="image" src="https://github.com/user-attachments/assets/97ed3c5b-4273-4903-abf1-0be70d1184f3" />

---

- we gonna calculate the Duration
- for calculating the duration we have to know the starting time and Ending time
- when we started loading the table and when we stoped loading the table

```
SET @start_time=GETDATE(); -- when we start loading this table
PRINT'>> Truncating Table:bronze.crm_cust_info '
Truncate table bronze.crm_cust_info;
-- Write SQL BULK insert to load all CSV Files
PRINT'>>Inserting Data Into:bronze.crm_cust_info '
BULK INSERT bronze.crm_cust_info
from 'E:\Extract_source_files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
 firstrow=2,
 fieldterminator=',',
 tablock
);
 SET @end_time=GETDATE(); -- when we end loading this table
-- Print Duration
PRINT'>> Load Duration:'+ CAST(DATEDUFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';
PRINT'>> Truncating Table:bronze.crm_prd_info '
```
---
<img width="432" height="127" alt="image" src="https://github.com/user-attachments/assets/cfe39c5e-d34e-4164-bf88-41b8b95bd772" />
---
## Calculating the Duration of loading all Bronze tables
---
<img width="491" height="647" alt="image" src="https://github.com/user-attachments/assets/dd82bc05-b353-47a7-9a8a-cccfef7e1068" />

---
- Calculation the duration of whole Bronge layer "Whole Batch"
<img width="475" height="502" alt="image" src="https://github.com/user-attachments/assets/02b923e7-0f68-42ec-8383-c684ee01dc43" />
---

## Document: Dataflow (Draw.io)
---
 <img width="946" height="427" alt="image" src="https://github.com/user-attachments/assets/d77c23ba-11eb-426b-b246-329ef29ca993" />
---
* **Data Lineage:** Data Lineage is the end-to-end visibility of data’s origin, movement, transformations, and destination.

## Second part of the project to build Silver Layer of the Project
- what we gonna do in it now
---
 ## Analysing: Explore & Understand Data
 ## Document: Draw Data Integration (Draw.io) 
---
<img width="1073" height="596" alt="image" src="https://github.com/user-attachments/assets/574e59a6-1bc6-418a-89b4-e43c6bd3ef22" />

---
- Explore & Understand the data
- visualizing tables of CRM csv files
---

<img width="296" height="518" alt="image" src="https://github.com/user-attachments/assets/05128960-06b9-4a0c-84cb-76738be2f9eb" />

---
## coding: Data Cleansing
## validating: Data Correcctness Checks
---
specification of Bronze layer
---
<img width="784" height="261" alt="image" src="https://github.com/user-attachments/assets/075387cf-f4c8-45a3-a5ba-e27b98c89b53" />

---
## Building data Defination language for Silver layer
- It is comparatively easy to build DDL  for silver layer comapare broze layer
- since because we are not making any changes while loading data from bronze layer to silver layer so the DDL will be same in the silver layer also
---

<img width="847" height="576" alt="image" src="https://github.com/user-attachments/assets/722d46a9-a18b-4cd5-8f79-6a871734aedc" />

---
- Here i created successfully  6 tables for silver layer also
<img width="947" height="747" alt="image" src="https://github.com/user-attachments/assets/a2502970-6dd5-4b79-b177-1c7f720d1be2" />
---

## Before loading data from broze layer to silver layer we have to check Quality issues in Bronze layer

-  build and Load CRM_cust_info
- we will take each table one by one and check the quality issues of each table one by one
---
- check for Nulls or Duplicates in Primary Key
- Expectation: No Reult
- 
