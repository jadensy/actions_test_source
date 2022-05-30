--no_cache

/*
Temp Tables vs Tables Size

CREATE OR REPLACE VIEW admin.v_table_disk_usage AS
SELECT
  schema, 
  "table", 
  size, 
  tbl_rows
FROM
  SVV_TABLE_INFO
ORDER BY size DESC
*/

WITH 
  tmp_tbl AS (
    SELECT 
      sum(size)/1024 AS tmp_tbl_gbytes
    FROM 
      admin.v_table_disk_usage
    WHERE "table" LIKE 'temp_%'),
  tbl as (
    SELECT 
      sum(size)/1024 AS tbl_gbytes
    FROM 
      admin.v_table_disk_usage
    WHERE "table" NOT LIKE 'temp_%')
SELECT 
  'tmp_tbl_gbytes' AS types,
  tmp_tbl_gbytes AS sizes
FROM 
  tmp_tbl, 
  tbl
UNION ALL
SELECT 
  'tbl_gbytes' AS types,
  tbl_gbytes AS sizes
FROM 
  tmp_tbl, 
  tbl