--no_cache

/*
Cluster Disk Usage

CREATE OR REPLACE VIEW admin.v_cluster_disk_usage AS
select
  sum(capacity) as capacity_mbytes, 
  sum(used) as used_mbytes, 
  (sum(capacity) - sum(used)) as free_mbytes 
from 
  stv_partitions where part_begin=0
*/

SELECT capacity_mbytes/1024 AS capacity_gbytes, used_mbytes/1024 AS used_gbytes, 10000 AS cluster_storage, cluster_storage - used_gbytes AS free_gbytes FROM admin.v_cluster_disk_usage