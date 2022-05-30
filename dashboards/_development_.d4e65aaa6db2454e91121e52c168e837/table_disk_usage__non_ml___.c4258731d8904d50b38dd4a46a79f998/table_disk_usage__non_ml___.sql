--no_cache
/*Table Disk Usage*/
SELECT * FROM admin.v_table_disk_usage
WHERE schema NOT LIKE 'ml_%'