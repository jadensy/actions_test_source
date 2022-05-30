--no_cache
SELECT TOP 10 schema, SUM(size)/1024 as size  FROM admin.v_table_disk_usage
GROUP BY schema
order by size desc