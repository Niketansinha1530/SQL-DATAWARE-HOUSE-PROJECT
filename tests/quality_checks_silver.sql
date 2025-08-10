/* =========================================================
   DATA VALIDATION & CLEANUP SCRIPT
   Purpose: Validate and standardize data in BRONZE layer 
            before loading into SILVER schema.
   ========================================================= */

/* ================================
   1. CRM Customer Info Validation
   ================================ */

-- 1.1 Preview raw bronze customer data
SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
FROM bronze.crm_cust_info;

-- 1.2 Deduplicate customers (keep latest by create date)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cst_material_status, -- Normalize marital status

    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr, -- Normalize gender
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1;

-- 1.3 Identify records with leading/trailing spaces in firstname
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 1.4 Check distinct gender values
SELECT DISTINCT
    cst_gndr,
    CASE
        WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS normalized_gender
FROM bronze.crm_cust_info;

-- 1.5 Check distinct marital status values
SELECT DISTINCT
    cst_material_status,
    CASE 
        WHEN cst_material_status = 'S' THEN 'Single'
        WHEN cst_material_status = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS normalized_marital_status
FROM bronze.crm_cust_info;


/* ================================
   2. CRM Product Info Validation
   ================================ */

SET search_path TO bronze;

-- 2.1 Standardize and derive product attributes
SELECT
    prd_id,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id, -- Category ID for join with ERP
    SUBSTRING(TRIM(prd_key), 7, LENGTH(prd_key))       AS prd_key, -- Product key
    prd_nm,
    COALESCE(prd_cost, 0)                              AS prd_cost, -- Null cost to 0
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
FROM crm_prd_info;

-- 2.2 Identify invalid category IDs
SELECT
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
    prd_nm
FROM crm_prd_info
WHERE REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_')
      NOT IN (SELECT id FROM erp_px_cat_g1v2);

-- 2.3 Check invalid or missing product costs
SELECT prd_cost
FROM crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- 2.4 Check invalid date ranges (end before start)
SELECT
    prd_key,
    prd_start_dt,
    prd_end_dt
FROM crm_prd_info
WHERE prd_end_dt < prd_start_dt;


/* ================================
   3. CRM Sales Details Validation
   ================================ */

-- 3.1 Standardize sales data and recalculate where necessary
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS TEXT) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS TEXT) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS TEXT) AS DATE)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
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

-- 3.2 Check product key foreign key violations
SELECT
    sls_ord_num,
    sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT pro_key FROM silver.crm_prd_info);

-- 3.3 Identify invalid dates
SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LENGTH(sls_due_dt::TEXT) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- 3.4 Check logical date ordering
SELECT
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- 3.5 Validate sales consistency (sales = qty * price)
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
   OR sls_sales IS NULL OR sls_sales <= 0
   OR sls_quantity IS NULL OR sls_quantity <= 0
   OR sls_price IS NULL OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/* ================================
   4. ERP Customer Validation
   ================================ */

-- 4.1 Normalize CID, validate birth date, and gender
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LENGTH(cid))
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


/* ================================
   5. ERP Location Validation
   ================================ */

-- 5.1 Remove dashes from CID, standardize country names
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
        WHEN TRIM(cntry) = 'DE'           THEN 'Germany'
        WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;


/* ================================
   6. ERP Category Validation
   ================================ */

-- 6.1 Check for missing category IDs in products table
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

-- 6.2 Distinct maintenance values
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

