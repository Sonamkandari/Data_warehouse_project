# Data_warehouse_project

Welcome to the Data Warehouse and Analytical Project repository.
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry-based practices in the data engineering and analytics. 
---
- the first thing is we needd to create a project plan
- for creating a project plan we will use Notion
- Notion will help you to organise your plans, ideas all-in-one place.
- For creating a successful project, we need to have a clear project plan.
- Right now, I am at the beginning phase of my project. So, I am using the Notion tool for creating a basic project plan for my project.
- By using the Notion, we will create the main phases and stages of the project.

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

  

  

