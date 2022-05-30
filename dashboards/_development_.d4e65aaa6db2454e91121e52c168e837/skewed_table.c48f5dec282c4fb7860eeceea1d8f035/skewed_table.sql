SELECT database, schema, "table", size, skew_rows	 FROM SVV_TABLE_INFO
WHERE skew_rows > 0
order by skew_rows desc