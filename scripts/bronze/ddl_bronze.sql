-- =======================================================================
-- 🚀 SQL Data Warehouse Project: Bronze Layer Table Creation & Loading
-- Description: DDL + Data Load (Truncate & Insert) for Bronze Layer
-- Author: Niketan Sinha
-- =======================================================================

-- 🧹 Drop Tables If They Already Exist
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- 🏗️ Create Tables in Bronze Layer
-- -----------------------------------------------------

-- 👤 Customer Info Table (CRM)
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- 📦 Product Info Table (CRM)
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- 🧾 Sales Details Table (CRM)
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- 🌍 Location Table (ERP)
CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

-- 🧓 Customer Demographics Table (ERP)
CREATE TABLE bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

-- 🏷️ Product Category Table (ERP)
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

-- 🔍 Preview Tables (Optional - For Testing)
-- SELECT * FROM bronze.crm_cust_info;
-- SELECT * FROM bronze.crm_prd_info;

-- =======================================================================
-- 🔁 Truncate + Load Data from CSV Files
-- These operations load fresh data into bronze tables from local CSVs
-- CSV HEADER option: Skips the header row
-- =======================================================================

-- 🧓 Load ERP Customer Demographics
TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
DELIMITER ',' CSV HEADER;

-- 🌍 Load ERP Location Info
TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
DELIMITER ',' CSV HEADER;

-- 🏷️ Load ERP Product Categories
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
DELIMITER ',' CSV HEADER;

-- 👤 Load CRM Customer Info
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
DELIMITER ',' CSV HEADER;

-- 📦 Load CRM Product Info
TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
DELIMITER ',' CSV HEADER;

-- 🧾 Load CRM Sales Details
TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM 'F:\Sql Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
DELIMITER ',' CSV HEADER;

-- =======================================================================
-- ⚙️ Notes:
-- ✅ Use CSV HEADER to ignore column names during import
-- ✅ Data is refreshed using Truncate + Insert (no duplication)
-- =======================================================================

-- 🛠️ Calling Stored Procedure (If Defined)
-- CALL bronze.load_bronze_tables();

-- 🧯 Error Handling (Example If Using PL/pgSQL)
-- BEGIN
--     -- Your truncate + copy logic here
-- EXCEPTION
--     WHEN OTHERS THEN
--         RAISE NOTICE 'Error occurred: %, SQL State: %', SQLERRM, SQLSTATE;
-- END;

-- 📅 Bonus: Use `age()` Function to Calculate Time Gaps Between Timestamps
-- Example: SELECT age(prd_end_dt, prd_start_dt) FROM bronze.crm_prd_info;
