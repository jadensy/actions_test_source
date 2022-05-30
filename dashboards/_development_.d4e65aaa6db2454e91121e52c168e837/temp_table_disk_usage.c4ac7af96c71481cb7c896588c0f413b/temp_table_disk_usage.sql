--no_cache
/*Temp Table Disk Usage*/
SELECT * FROM admin.v_table_disk_usage
WHERE "table" like 'temp_%'