SELECT database, schema, "table", size, stats_off FROM SVV_TABLE_INFO
WHERE stats_off > 0
order by stats_off desc