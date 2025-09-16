--CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
--EXPECTATION: No Result
SELECT 
cid, count(*)
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

--CHECK FOR UNWANTED SPACES
--EXPECTATION: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--CHECK FOR NULLS OR NEGATIVE NUMBERS
--EXPECTATION: No Result
SELECT * FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT TRIM(maintenance) FROM bronze.erp_px_cat_g1v2


--DATA ENRICHMENT: CHEK FOR INVALID DATE ORDERS
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt < prd_end_dt

--CHECK FOR INVALID DATE
SELECT distinct 
sls_order_dt
,LENGTH(CAST(sls_order_dt AS VARCHAR))
FROM silver.crm_sales_details
WHERE sls_order_dt < 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT BDATE AS OLD_BDATE,
CASE WHEN BDATE > NOW() THEN NULL ELSE BDATE END AS BDATE
FROM silver.erp_cust_az12
WHERE bdate > now()

--CHECK FOR BUSINESS RULES (SALES = QUANTITY*PRICE)
SELECT DISTINCT sls_sales, sls_quantity, sls_price FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity, sls_price
;

--SLS QUANTITY HAS GOOD QUALITY
SELECT DISTINCT sls_sales, sls_quantity, sls_price FROM bronze.crm_sales_details
WHERE sls_price IS NULL AND sls_sales IS NULL


SELECT * FROM silver.erp_px_cat_g1v2



