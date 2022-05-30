--no_cache

/*
Tables vs Non Tables Size

CREATE OR REPLACE VIEW admin.v_cluster_disk_usage AS
select
  sum(capacity) as capacity_mbytes, 
  sum(used) as used_mbytes, 
  (sum(capacity) - sum(used)) as free_mbytes 
from 
  stv_partitions where part_begin=0

CREATE OR REPLACE VIEW admin.v_database_disk_usage AS
WITH
  blocks as (
    SELECT 
      tbl, 
      COUNT(*) as mbytes
    FROM 
      stv_blocklist 
    GROUP BY tbl  
  ),
  database_stat as (
    SELECT 
      TRIM(pgdb.datname) as Database,
      TRIM(perm_tbl.name) AS Table, 
      blocks.mbytes
    FROM 
      stv_tbl_perm perm_tbl
    JOIN pg_database as pgdb 
    ON pgdb.oid = perm_tbl.db_id
    JOIN blocks 
    ON perm_tbl.id=blocks.tbl
    WHERE perm_tbl.slice=0
    ORDER BY db_id, name  
  )           
SELECT 
  SUM(mbytes) AS mbyte_storage,
  database 
FROM
  database_stat
GROUP BY database
ORDER BY mbyte_storage DESC
*/

WITH
  du AS(
    SELECT 
      sum(mbyte_storage) as table_mb 
    FROM 
      admin.v_database_disk_usage)
SELECT 
  'tbl_gbytes' AS types,
  table_mb/1024 AS sizes
FROM 
  admin.v_cluster_disk_usage, 
  du
UNION ALL
SELECT 
  'non_tbl_gbytes' AS types, 
  (used_mbytes - table_mb)/1024 as sizes
FROM 
  admin.v_cluster_disk_usage, 
  du