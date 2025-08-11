/*
==========================================================
 Description   : This script creates the Gold Layer views 
                 for the Data Warehouse, including:
                   - dim_customers
                   - dim_products
                   - fact_sales
 Author        : Niketan Sinha
 Last Updated  : 8/11/2025
==========================================================
*/

----------------------------------------------------------
-- CREATE VIEW: gold.dim_customers
-- Purpose: Customer dimension table with surrogate keys,
--          enriched customer info from multiple silver tables.
----------------------------------------------------------
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,       -- Surrogate Key
    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    ci.cst_material_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,                                             -- Gender enrichment
    lc.cntry           AS country,
    ca.bdate           AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS lc
    ON ci.cst_key = lc.cid;


----------------------------------------------------------
-- CREATE VIEW: gold.dim_products
-- Purpose: Product dimension table enriched with category 
--          details and filtered to exclude historical products.
----------------------------------------------------------
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key, -- Surrogate Key
    pr.prd_id      AS product_id,
    pr.prd_key     AS product_number,
    pr.prd_nm      AS product_name,
    pr.cat_id      AS category_id,
    px.cat         AS category,
    px.subcat      AS subcategory,
    pr.prd_line    AS product_line,
    px.maintenance AS maintenance,
    pr.prd_cost    AS product_cost,
    pr.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_px_cat_g1v2 AS px
    ON pr.cat_id = px.id
WHERE prd_end_dt IS NULL;  -- Filter out all historical product data


----------------------------------------------------------
-- CREATE VIEW: gold.fact_sales
-- Purpose: Fact table containing sales transactions linked 
--          to product and customer dimensions.
----------------------------------------------------------
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pd.product_key,
    cs.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pd
    ON sd.sls_prd_key = pd.product_number
LEFT JOIN gold.dim_customers AS cs
    ON sd.sls_cust_id = cs.customer_id;
