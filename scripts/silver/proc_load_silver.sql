/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
	DECLARE start_time TIMESTAMP; end_time TIMESTAMP; batch_start_time TIMESTAMP; batch_end_time TIMESTAMP;
	
	BEGIN
		batch_start_time := NOW();
		
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'LOADING SILVER LAYER.....';
		RAISE NOTICE '==========================================';
		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING CRM DATA:';
		RAISE NOTICE '------------------------------------------';

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();
		
		RAISE NOTICE 'TRUNCATING silver.crm_cust_info';
		TRUNCATE silver.crm_cust_info;
		RAISE NOTICE 'INSERTING DATA silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date
		)
		SELECT 
		cst_id,cst_key,TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname, --Remove unwanted spaces
		
		CASE UPPER(TRIM(cst_marital_status))
		WHEN 'M' THEN 'Married' --Data Normalization and Standardization
		WHEN 'S' THEN 'Single'
		ELSE 'N/A' END AS cst_marital_status --Handling Missing Values
		,
		CASE UPPER(TRIM(cst_gndr))
		WHEN 'F' THEN 'Female'
		WHEN 'M' THEN 'Male'
		ELSE 'N/A' END AS cst_gndr
		
		,cst_create_date
			FROM(
			SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
		)t WHERE flag_last =1; --Remove duplicates


		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
		
		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING silver.crm_prd_info';
		TRUNCATE silver.crm_prd_info;
		RAISE NOTICE 'INSERTING DATA INTO silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
		prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- TO MATCH erp id
		SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key, -- TO MATCH crm prd_key
		prd_nm,
		COALESCE(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road' 
		WHEN 'S' THEN 'Other Sales'
		ELSE 'N/A' END AS prd_line,
		CAST (prd_start_dt AS DATE),
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 day' AS DATE)
		AS prd_end_dt
		FROM bronze.crm_prd_info;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING silver.crm_sales_details';
		TRUNCATE silver.crm_sales_details;
		RAISE NOTICE 'INSERTING DATA INTO silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
		)
		SELECT
		sls_ord_num,sls_prd_key,sls_cust_id,
		CASE WHEN sls_order_dt < 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END AS sls_order_dt
		,
		CASE WHEN sls_ship_dt < 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END AS sls_ship_dt
		,
		CASE WHEN sls_due_dt < 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END AS sls_due_dt
		,
		--RULE 1: IF SALES IS NEGATIVE, ZERO OR NULL, DERIVE USING QUANTITY * PRICE
		--RULE 2: IF PRICE IS NEGATIVE, ZERO OR NULL, DERIVE USING SALES/ QUANTITY
		CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales
		,sls_quantity,
		CASE WHEN sls_price <=0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
		   	 ELSE sls_price
		END AS sls_price
		
		FROM bronze.crm_sales_details;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING ERP DATA:';
		RAISE NOTICE '------------------------------------------';

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING silver.erp_cust_az12';		
		TRUNCATE silver.erp_cust_az12;
		RAISE NOTICE 'INSERTING DATA INTO silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			CID,BDATE,GEN
		)
		SELECT REPLACE(CID,'NAS','') AS CID,
		CASE WHEN BDATE > NOW() 
			THEN NULL 
			ELSE BDATE
		END AS BDATE
			,
		CASE 
			WHEN UPPER(TRIM(GEN)) = 'M' OR UPPER(TRIM(GEN)) = 'MALE' THEN 'Male'
			WHEN UPPER(TRIM(GEN)) = 'F' OR UPPER(TRIM(GEN)) = 'FEMALE' THEN 'Female'
			ELSE 'N/A'
		END AS GEN
		FROM bronze.erp_cust_az12;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING silver.erp_loc_a101';
		TRUNCATE silver.erp_loc_a101;
		RAISE NOTICE 'INSERTING DATA INTO silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			CID,CNTRY
		)
		SELECT 
		REPLACE(CID,'-','') AS CID,
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) = 'USA' OR TRIM(CNTRY) = 'US' THEN 'United States'
			 WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = '' THEN 'N/A'
			 ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM bronze.erp_loc_a101;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING silver.erp_px_cat_g1v2';
		TRUNCATE silver.erp_px_cat_g1v2;
		RAISE NOTICE 'INSERTING DATA INTO silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			ID,CAT,SUBCAT,MAINTENANCE
		)
		SELECT ID,CAT,SUBCAT,MAINTENANCE
		FROM bronze.erp_px_cat_g1v2;
		end_time :=NOW();
		
		RAISE NOTICE '>> Load Duration : % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		batch_end_time := NOW();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Silver Layer is successfully loaded.';
		RAISE NOTICE '	- Total Load Duration: % ms', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)) * 1000;
		RAISE NOTICE '==========================================';

	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE '=============================================';
			RAISE NOTICE 'ERROR OCCURED DURING LOADING SILVER LAYER';
			RAISE NOTICE 'Error message: %', SQLERRM;
	END;
		
END; $$
