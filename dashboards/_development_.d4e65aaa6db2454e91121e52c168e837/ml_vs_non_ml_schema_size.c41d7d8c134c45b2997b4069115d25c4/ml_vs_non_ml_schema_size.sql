--no_cache
WITH
  tbl_size AS(
    SELECT 
      SUM(size)/1024 AS non_ml_size 
    FROM 
      admin.v_table_disk_usage
    WHERE schema NOT LIKE 'ml_%'),
  tbl_size_ml AS(
    SELECT 
      SUM(size)/1024 AS ml_size 
    FROM 
      admin.v_table_disk_usage)
SELECT 
  'ml_size' as types, 
  (ml_size - non_ml_size) AS sizes 
FROM 
  tbl_size, 
  tbl_size_ml
UNION ALL
SELECT 
  'non_ml_size' AS types,
  non_ml_size AS sizes
FROM 
  tbl_size, 
  tbl_size_ml