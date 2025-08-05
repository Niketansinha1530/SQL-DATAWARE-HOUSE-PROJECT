-- =======================================================================
-- 🚀 Stored Procedure: bronze.load_bronze_tables
-- Description: Truncates and loads all Bronze Layer tables from CSV files
--              with logging, duration tracking, and error handling.
-- Author: Niketan Sinha
-- =======================================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    -- 🕒 Timestamps to track load durations
    start_time        TIMESTAMP;
    end_time          TIMESTAMP;
    batch_start_time  TIMESTAMP;
    batch_end_time    TIMESTAMP;
BEGIN
    -- ⏱️ Log the overall start of batch execution
    batch_start_time := clock_timestamp();

    RAISE NOTICE '===========================================';
    RAISE NOTICE '✅ Starting data load into bronze tables...';
    RAISE NOTICE '===========================================';

    -- =============================
    -- 🔁 TRY Block for Safe Execution
    -- =============================
    BEGIN

        -- =========================
        -- 🔄 Load: ERP - CUST_AZ12
        -- =========================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading ERP: bronze.erp_cust_az12';

        TRUNCATE TABLE bronze.erp_cust_az12;

        COPY bronze.erp_cust_az12
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for erp_cust_az12: %', age(end_time, start_time);

        -- =========================
        -- 🔄 Load: ERP - LOC_A101
        -- =========================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading ERP: bronze.erp_loc_a101';

        TRUNCATE TABLE bronze.erp_loc_a101;

        COPY bronze.erp_loc_a101
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for erp_loc_a101: %', age(end_time, start_time);

        -- =============================
        -- 🔄 Load: ERP - PX_CAT_G1V2
        -- =============================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading ERP: bronze.erp_px_cat_g1v2';

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        COPY bronze.erp_px_cat_g1v2
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for erp_px_cat_g1v2: %', age(end_time, start_time);

        -- ================================
        -- 🔄 Load: CRM - CRM_CUST_INFO
        -- ================================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading CRM: bronze.crm_cust_info';

        TRUNCATE TABLE bronze.crm_cust_info;

        COPY bronze.crm_cust_info
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for crm_cust_info: %', age(end_time, start_time);

        -- ================================
        -- 🔄 Load: CRM - CRM_PRD_INFO
        -- ================================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading CRM: bronze.crm_prd_info';

        TRUNCATE TABLE bronze.crm_prd_info;

        COPY bronze.crm_prd_info
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for crm_prd_info: %', age(end_time, start_time);

        -- =====================================
        -- 🔄 Load: CRM - CRM_SALES_DETAILS
        -- =====================================
        start_time := clock_timestamp();
        RAISE NOTICE 'Truncating and loading CRM: bronze.crm_sales_details';

        TRUNCATE TABLE bronze.crm_sales_details;

        COPY bronze.crm_sales_details
        FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE '⏱ Duration for crm_sales_details: %', age(end_time, start_time);

        -- ✅ Overall Load Complete
        batch_end_time := clock_timestamp();
        RAISE NOTICE '===========================================================';
        RAISE NOTICE '✅ Duration for loading all Bronze Tables: %', age(batch_end_time, batch_start_time);
        RAISE NOTICE '===========================================================';

    -- ================================
    -- ❌ EXCEPTION Block (Error Catch)
    -- ================================
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION '❌ Error occurred: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;

END;
$$;
