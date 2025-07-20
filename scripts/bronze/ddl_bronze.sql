
create or alter procedure bronze.load_bronze as
begin
	begin try
if object_id('bronze.crm_cust_info','U') is not null
	Drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);
if object_id('bronze.crm_prd_info','U') is not null
	Drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime 
);
if object_id('bronze.crm_sales_details','U') is not null
	Drop table bronze.crm_sales_details
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
if object_id('bronze.erp_loc_a101','U') is not null
	Drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),cntry nvarchar(50));
if object_id('bronze.erp_cust_az12','U') is not null
	Drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,gen nvarchar(50)
);
if object_id('bronze.erp_px_cat_g1v2','U') is not null
	Drop table bronze.erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);	
	Declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
;
	set @batch_start_time=getdate();

	print'=======================';
	print'Loading Bronze layer';
	print'=======================';

	print'------------------------';
	print'Loading CRM Tables';
	print'------------------------';

	set @start_time=GetDate();	
	print'>>Truncating Table:bronze.crm_cust_info';
truncate table bronze.crm_cust_info;
	print'>>Inserting Table:bronze.crm_cust_info';
bulk insert bronze.crm_cust_info
from 'C:\SQLDatasets\source_crm\cust_info.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------------';


set @start_time=GetDate();
print'>>Truncating Table:bronze.crm_prd_info';
truncate table bronze.crm_prd_info;
print'>>Inserting Table:bronze.crm_prd_info';
bulk insert bronze.crm_prd_info
from 'C:\SQLDatasets\source_crm\prd_info.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------------';

set @start_time=GetDate();

print'>>Truncating Table:bronze.crm_sales_details';
truncate table bronze.crm_sales_details;
print'>>Inserting Table:bronze.crm_sales_details';
bulk insert bronze.crm_sales_details
from 'C:\SQLDatasets\source_crm\sales_details.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------';

	print'------------------------';
	print'Loading ERP Tables';
	print'------------------------';

set @start_time=GetDate();
print'>>Truncating Table:bronze.erp_loc_a101';

truncate table bronze.erp_loc_a101;
print'>>Inserting Table:bronze.erp_loc_a101';
bulk insert bronze.erp_loc_a101
from 'C:\SQLDatasets\source_erp\loc_a101.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------';
print'>>Truncating Table:bronze.erp_cust_az12';


set @start_time=GetDate();
truncate table bronze.erp_cust_az12;
print'>>Inserting Table:bronze.erp_cust_az12';
bulk insert bronze.erp_cust_az12
from 'C:\SQLDatasets\source_erp\cust_az12.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------';

print'>>Truncating Table:bronze.erp_px_at_g1v2';

set @start_time=GetDate();
truncate table bronze.erp_px_cat_g1v2;
print'>>Inserting Table:bronze.erp_px_at_g1v2';
bulk insert bronze.erp_px_cat_g1v2
from 'C:\SQLDatasets\source_erp\px_cat_g1v2.csv'
with(
	firstrow=2,
	fieldterminator=',',
	tablock
);
set @end_time=GetDate();
print '>>Load Duration: '+cast(DateDiff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
print'------';
set @batch_end_time=getdate();
print'=======================';
print'Loading Bronze layer is completed';
print'--Load duration '+cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+ ' Seconds';
print'============================';
end try
begin catch
	print'==========';
	print'Error Occured dutrng loading bronze layer';
	print'Error Message'+Error_message();
	print'Error Message'+cast(Error_number() as nvarchar);
	print'Error Message'+cast(Error_state() as nvarchar);
	print'==========';
end catch
end
