--no_cache
/**********************************************************************************************
Purpose: View to identify how frequently queries scan database tables. 
Data queried is for the last 3 months
History:
2016-03-14 chriz-bigdata Created
**********************************************************************************************/
select * from admin.v_get_tbl_scan_frequency