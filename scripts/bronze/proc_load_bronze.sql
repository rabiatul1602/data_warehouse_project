/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
	DECLARE start_time TIMESTAMP; end_time TIMESTAMP; batch_start_time TIMESTAMP; batch_end_time TIMESTAMP;
	
	BEGIN
		batch_start_time := NOW();

		RAISE NOTICE '==========================================';
		RAISE NOTICE 'LOADING BRONZE LAYER.....';
		RAISE NOTICE '==========================================';
		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING CRM DATA:';
		RAISE NOTICE '------------------------------------------';

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();
		RAISE NOTICE 'TRUNCATING bronze.crm_cust_info';
		TRUNCATE bronze.crm_cust_info;
		RAISE NOTICE 'INSERTING DATA bronze.crm_cust_info';
		COPY bronze.crm_cust_info (
			cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date
		) 
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
		
		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING bronze.crm_prd_info';
		TRUNCATE bronze.crm_prd_info;
		RAISE NOTICE 'INSERTING DATA INTO bronze.crm_prd_info';
		COPY bronze.crm_prd_info (
			prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
		)
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING bronze.crm_sales_details';
		TRUNCATE bronze.crm_sales_details;
		RAISE NOTICE 'INSERTING DATA INTO bronze.crm_sales_details';
		COPY bronze.crm_sales_details (
			sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
		)
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING ERP DATA:';
		RAISE NOTICE '------------------------------------------';

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING bronze.erp_cust_az12';
		TRUNCATE bronze.erp_cust_az12;
		RAISE NOTICE 'INSERTING DATA INTO bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12 (
			CID,BDATE,GEN
		)
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING bronze.erp_loc_a101';
		TRUNCATE bronze.erp_loc_a101;
		RAISE NOTICE 'INSERTING DATA INTO bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101 (
			CID,CNTRY
		)
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration: % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		RAISE NOTICE '------------------------------------------';
		start_time := NOW();

		RAISE NOTICE 'TRUNCATING bronze.erp_px_cat_g1v2';
		TRUNCATE bronze.erp_px_cat_g1v2;
		RAISE NOTICE 'INSERTING DATA INTO bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2 (
			ID,CAT,SUBCAT,MAINTENANCE
		)
		FROM 'C:\Program Files\PostgreSQL\17\data\staging\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;

		end_time :=NOW();
		RAISE NOTICE '>> Load Duration : % ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;

		batch_end_time := NOW();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Bronze Layer is successfully loaded.';
		RAISE NOTICE '	- Total Load Duration: % ms', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time)) * 1000;
		RAISE NOTICE '==========================================';
		
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE '=============================================';
			RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			RAISE NOTICE 'Error message: %', SQLERRM;
	END;

END; $$
