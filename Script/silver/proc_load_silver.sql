/*
what i had done here
============================================================================
stored procedure: load silver layer (Bronze-> silver )
============================================================================
what my Script is doing:
     This script procedure performs the ETL (Extract,Transform,Load) Process
	 to populate the 'silver' schema tables from the 'bronze'schema
Actions Performed:
     - Truncate silver tables
	 - Inserts transformed and cleared data from Bronze into silver tables
Parameters:
      None.
	  This stored procedure does not accept any parameters or return any values,
Usage example;
       EXEC silver.load_silver;
==============================================================================
*/


-- Created stored procedures
CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
    -- Adding duration time of each table and also Duration time of complete silver table
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    SET @batch_start_time=GETDATE();
    BEGIN TRY

        PRINT'========================================';
        PRINT'Loading Bronze Layer'
        PRINT'========================================';

        PRINT'------------------------------------------------------------------------'
        PRINT'Inserting cleaned data into silver layer from Bronze layer of CRM Tables'
        PRINT'------------------------------------------------------------------------'
        -- inserting all the cleaned data into silver schema
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into:silver.crm_cust_info';
        -- NOW INSERTING ALL THE CLEANED COLUMN OF crm_cust_info  IN THE SILVER TABLE
        SET @start_time=GETDATE();
        INSERT INTO silver.crm_cust_info(
         cst_id,
         cst_key,
         cst_firstname,
         cst_lastname,
         cst_marital_status,
         cst_gndr,
         cst_create_date
         )
        -- quality check for unwanted spaces in string values
        -- now we have to write transformation
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,

            CASE 
                WHEN cst_marital_status = 'S' THEN 'Single'
                WHEN cst_marital_status = 'M' THEN 'Married'   -- fixed spelling
                ELSE 'secret'
            END AS cst_marital_status,

            CASE
                WHEN cst_gndr = 'F' THEN 'Female'
                WHEN cst_gndr = 'M' THEN 'Male'
                ELSE 'Other_gender'
            END AS cst_gndr,

            cst_create_date

        FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
             ) t
        WHERE flag_last = 1;
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into:silver.crm_prd_info';
        -- NOW INSERTING ALL THE CLEANED COLUMN OF crm_prd_info  IN THE SILVER TABLE
        SET @start_time=GETDATE();
        INSERT INTO silver.crm_prd_info (
            prd_id,
            prd_key,
            cat_id,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt,
            dwh_create_date
        )
        SELECT 
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'NA'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(
                    DAY,
                    -1,
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
                ) AS DATE
            ) AS prd_end_dt,
            GETDATE() AS dwh_create_date
        FROM bronze.crm_prd_info;
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';


        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into:silver.crm_sales_details';
        --  NOW INSERTING ALL THE CLEANED COLUMN OF crm_sales_details  IN THE SILVER TABLE
        SET @start_time=GETDATE();
        INSERT INTO silver.crm_sales_details (
           sls_ord_num,
           sls_prd_key,
           sls_cus_id,
           sls_order_dt,
           sls_ship_dt,
           sls_due_dt,
           sls_sales,
           sls_quantity,
           sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cus_id,

            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
            END AS sls_order_dt,

            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
            END AS sls_ship_dt,

            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
                THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
            END AS sls_due_dt,

            CASE 
                WHEN sls_sales IS NULL 
                  OR sls_sales <= 0 
                  OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price

        FROM bronze.crm_sales_details;
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';

        PRINT'------------------------------------------------------------------------'
        PRINT'Inserting cleaned data into silver layer from Bronze layer of erp Tables'
        PRINT'------------------------------------------------------------------------'

        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into:silver.erp_cust_az12';
        -- NOW INSERTING ALL THE CLEANED COLUMN OF erp_cust_az12 IN THE SILVER TABLE
        SET @start_time=GETDATE();
        Insert into silver.erp_cust_az12(
        cid,
        bdate,
        gen
        )
        select
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	        ELSE  CID
        END AS cid,

        -- Writing transformation for earth date
        CASE WHEN bdate > GETDATE() THEN NULL
	        ELSE bdate
        END as bdate,
        CASE  
	        when UPPER(TRIM(gen))  IN ('F','FEMALE') THEN 'Female'
	        when UPPER(TRIM(gen)) IN ('M','MALE')THEN 'Male'
        ELSE  'n/a'
        END as gen
        from bronze.erp_cust_az12
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';


        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting Data Into:silver.erp_loc_a101';

        -- NOW INSERTING ALL THE CLEANED COLUMN OF erp_loc_a101 IN THE SILVER TABLE
        SET @start_time=GETDATE();
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'NA'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';

        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into:silver.erp_px_cat_g1v2';
        --NOW INSERTING ALL THE CLEANED COLUMN OF erp_px_cat_g1v2 IN THE SILVER TABLE
        SET @start_time=GETDATE();
        INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance)

        select id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2;
        SET @end_time=GETDATE();
        PRINT'>>Load Duration:'+CAST(DATEdIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
        PRINT'>>----------------';

        -- Print load Duration of complete silver table
        PRINT'>> Load Duration:' +CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+ 'seconds';
        PRINT'>>--------------------';
        SET @batch_end_time=GETDATE();
        PRINT'Loading silver layer is Completed';
        PRINT '>> Total Load Duration: ' 
      + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR)
      + ' seconds';

        PRINT'==================================================='

END TRY
BEGIN CATCH
-- we can design what can happen if there is exist any error in the silver layer
 PRINT'-------------------------------------------------------'
 PRINT'ERROR occoured during inserting data into silver layer'
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
 PRINT'-------------------------------------------------------'

END CATCH
END
