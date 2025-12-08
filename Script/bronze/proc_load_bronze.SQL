/*
===================================================
Stored procedure : load Bronze layer (Source-> Bronze)
===================================================
*/

-- creating stored procedures
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
    SET @batch_start_time=GETDATE();
    BEGIN  TRY

        PRINT'=========================================';
        PRINT'Loading Bronze Layer';
        PRINT'=========================================';



        PRINT'-----------------------------------------';
        PRINT'Loading CRM Tables';
        PRINT'-----------------------------------------';

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
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';

        SET @start_time=GETDATE(); -- when we start loading this table
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
        SET @end_time=GETDATE(); -- when we end loading this table
        -- Print Duration
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';


        SET @start_time=GETDATE(); -- when we start loading this table
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
        SET @end_time=GETDATE(); -- when we end loading this table
        -- Print Duration
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';


        PRINT'-----------------------------------------';
        PRINT'Loading ERP Tables';
        PRINT'-----------------------------------------';

        -- load the data of erp also
        SET @start_time=GETDATE(); -- when we start loading this table
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
        SET @end_time=GETDATE(); -- when we end loading this table
        -- Print Duration
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';

        SET @start_time=GETDATE(); -- when we start loading this table
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
         SET @end_time=GETDATE(); -- when we end loading this table
        -- Print Duration
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';

        SET @start_time=GETDATE(); -- when we start loading this table
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
        SET @end_time=GETDATE(); -- when we end loading this table
        -- Print Duration
        PRINT'>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ 'seconds';
        PRINT'>>---------------';
        SET @batch_end_time=GETDATE();
        PRINT'====================================='
        PRINT'Loading Bronze Layer is Completed';
        PRINT'Total Duration:'+CAST(DATEDIFF(
        second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
        PRINT'====================================='
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
    -- we can Design what can happen if there exist a error in the Bronze layer
        PRINT'---------------------------------------------'
        PRINT'Error occured during loading bronze layer'
        PRINT'Error Message'+ERROR_MESSAGE();
        PRINT'Error Message'+CAST (ERROR_NUMBER()AS NVARCHAR);
        PRINT'ERROR Message'+CAST(ERROR_STATE()AS NVARCHAR);
        PRINT'---------------------------------------------'
    END CATCH
END

