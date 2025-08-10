/**************************************************************************************************
 * File Name: ddl_silver.sql
 * Description: This script creates all tables in the "silver" schema for CRM and ERP data storage.
 *              The Silver layer stores cleansed and standardized data, ready for further processing
 *              and analysis in the Gold layer.
 *
 * Tables Created:
 *   CRM:
 *     - silver.crm_cust_info       : Stores cleaned customer details.
 *     - silver.crm_prd_info        : Stores cleaned product details.
 *     - silver.crm_sales_details   : Stores cleaned sales transaction details.
 *
 *   ERP:
 *     - silver.erp_loc_a101        : Stores cleaned customer location details.
 *     - silver.erp_cust_az12       : Stores cleaned customer birth date & gender details.
 *     - silver.erp_px_cat_g1v2     : Stores cleaned product category details.
 *
 * Author: Niketan
 * Date Created: 8/10/2025
 **************************************************************************************************/

/*==============================================================================================
  CRM TABLES
==============================================================================================*/

-- Drop and recreate table: silver.crm_cust_info
-- Purpose: Stores cleaned and deduplicated customer master data from the Bronze layer.
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,             -- Customer ID
    cst_key             VARCHAR(50),     -- Customer Key
    cst_firstname       VARCHAR(50),     -- First Name
    cst_lastname        VARCHAR(50),     -- Last Name
    cst_material_status VARCHAR(50),     -- Marital Status (normalized)
    cst_gndr            VARCHAR(50),     -- Gender (normalized)
    cst_create_date     DATE,            -- Customer Creation Date
    dwh_create_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Record Creation Timestamp in DWH
);

-- Drop and recreate table: silver.crm_prd_info
-- Purpose: Stores cleaned product master data with category and product line mapping.
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id          INT,             -- Product ID
    cat_id          VARCHAR(50),     -- Category ID (derived)
    prd_key         VARCHAR(50),     -- Product Key
    prd_nm          VARCHAR(50),     -- Product Name
    prd_cost        INT,              -- Product Cost
    prd_line        VARCHAR(50),     -- Product Line (normalized)
    prd_start_dt    DATE,             -- Product Start Date
    prd_end_dt      DATE,             -- Product End Date
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Drop and recreate table: silver.crm_sales_details
-- Purpose: Stores cleaned sales transaction details with corrected sales and pricing data.
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num     VARCHAR(50),     -- Sales Order Number
    sls_prd_key     VARCHAR(50),     -- Product Key (FK to crm_prd_info)
    sls_cust_id     INT,              -- Customer ID (FK to crm_cust_info)
    sls_order_dt    DATE,             -- Order Date
    sls_ship_dt     DATE,             -- Shipping Date
    sls_due_dt      DATE,             -- Due Date
    sls_sales       INT,              -- Total Sales Amount
    sls_quantity    INT,              -- Quantity Sold
    sls_price       INT,              -- Price per Unit
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


/*==============================================================================================
  ERP TABLES
==============================================================================================*/

-- Drop and recreate table: silver.erp_loc_a101
-- Purpose: Stores cleaned customer location data.
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),     -- Customer ID
    cntry           VARCHAR(50),     -- Country (normalized)
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Drop and recreate table: silver.erp_cust_az12
-- Purpose: Stores cleaned customer demographic data (birth date & gender).
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),     -- Customer ID
    bdate           DATE,             -- Birth Date
    gen             VARCHAR(50),     -- Gender (normalized)
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Drop and recreate table: silver.erp_px_cat_g1v2
-- Purpose: Stores cleaned product category and subcategory data.
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR(50),     -- Product Category ID
    cat             VARCHAR(50),     -- Category Name
    subcat          VARCHAR(50),     -- Subcategory Name
    maintenance     VARCHAR(50),     -- Maintenance Category
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
