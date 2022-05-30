SELECT database, schema, "table", size, diststyle, sortkey1	 FROM SVV_TABLE_INFO
WHERE (sortkey1 is null
OR diststyle NOT LIKE 'KEY%')
AND schema NOT LIKE 'usr_%'
order by size desc