SELECT database, schema, "table", size, sortkey1, unsorted FROM SVV_TABLE_INFO
WHERE unsorted > 0
order by unsorted desc