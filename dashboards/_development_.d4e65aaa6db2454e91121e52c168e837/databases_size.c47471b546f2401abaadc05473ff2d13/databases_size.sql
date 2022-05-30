--no_cache

/*
To query disk storage usage per databases in GB

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
SELECT 
  mbyte_storage/1024 as storage_gb, 
  database 
FROM 
  admin.v_database_disk_usage