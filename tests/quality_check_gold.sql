/*
==========================================================
 Description   : Validation and testing queries for Gold 
                 Layer in the Data Warehouse project.
                 Includes:
                   - Source table checks
                   - Duplicate detection
                   - Data consistency checks
                   - Foreign key integrity validation
 Author        : Niketan Sinha
 Last Updated  : 8/11/2025
==========================================================
*/

----------------------------------------------------------
-- 1. View raw source tables from Silver Layer
----------------------------------------------------------
SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_loc_a101;

----------------------------------------------------------
-- 2. Check for duplicates in Customer data after join
----------------------------------------------------------
SELECT
    customer_id,
    COUNT(*) AS record_count
FROM (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer,   -- Surrogate Key preview
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname  AS last_name,
        ci.cst_material_status AS marital_status,
        CASE
            WHEN cst_gndr != 'n/a' THEN cst_gndr
            ELSE gen
        END AS gender,
        lc.cntry AS country,
        ca.bdate AS birthdate,
        ci.cst_create_date AS create_date
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS lc
        ON ci.cst_key = lc.cid
) AS t
GROUP BY customer_id
HAVING COUNT(*) > 2;

----------------------------------------------------------
-- 3. Check for gender mismatch between source tables
--    (Identify master data source and set priority)
----------------------------------------------------------
-- All different combinations of gender values
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid;

-- Unique gender values from CRM
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;

-- Unique gender values from ERP
SELECT DISTINCT gen FROM silver.erp_cust_az12;

-- Final gender mapping logic for Gold Layer
SELECT
    ci.cst_gndr,
    ca.gen,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid;

----------------------------------------------------------
-- 4. Gold Layer Customer Dimension checks
----------------------------------------------------------
SELECT * FROM gold.dim_customers;
SELECT DISTINCT gender FROM gold.dim_customers;

----------------------------------------------------------
-- 5. Product Data checks from Silver Layer
----------------------------------------------------------
SELECT * FROM silver.crm_prd_info;
SELECT * FROM silver.erp_px_cat_g1v2;

-- Join with category details, exclude historical products
SELECT
    pr.prd_id,
    pr.cat_id,
    px.cat,
    px.subcat,
    pr.prd_key,
    pr.prd_nm,
    pr.prd_cost,
    pr.prd_line,
    pr.prd_start_dt,
    px.maintenance
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_px_cat_g1v2 AS px
    ON pr.cat_id = px.id
WHERE prd_end_dt IS NULL;

----------------------------------------------------------
-- 6. Check for duplicate Product IDs
----------------------------------------------------------
SELECT
    product_id,
    COUNT(*) AS record_count
FROM (
    SELECT
        pr.prd_id AS product_id,
        pr.cat_id,
        px.cat,
        px.subcat,
        pr.prd_key,
        pr.prd_nm,
        pr.prd_cost,
        pr.prd_line,
        pr.prd_start_dt,
        px.maintenance
    FROM silver.crm_prd_info AS pr
    LEFT JOIN silver.erp_px_cat_g1v2 AS px
        ON pr.cat_id = px.id
    WHERE prd_end_dt IS NULL
) AS t
GROUP BY product_id
HAVING COUNT(*) > 1;

----------------------------------------------------------
-- 7. Gold Layer Product Dimension check
----------------------------------------------------------
SELECT * FROM gold.dim_products;
SELECT * FROM gold.dim_customers;

----------------------------------------------------------
-- 8. Foreign Key Integrity Checks (Fact Table)
----------------------------------------------------------
SELECT * FROM gold.fact_sales;

-- Validate joins between fact and dimension tables
SELECT
    *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;  -- Missing product references
