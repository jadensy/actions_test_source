--no_cache
/**********************************************************************************************
Purpose: Return the Current queries running and queueing, along with resource consumption.
Columns:
user :			User name
pid :			Pid of the session
xid :			Transaction identity
query :			Query Id
q :				Queue
slt :			Slots Uses
start :			Time query was issued
state :			Current State
q_sec :			Seconds in queue
exe_sec :		Seconds Executed
cpu_sec :		CPU seconds consumed
read_mb :		MB read by the query
spill_mb :		MB spilled to disk
ret_rows :		Rows returned to Leader -> Client
nl_rows :		# of rows of Nested Loop Join
sql :			First 90 Characters of the query SQL
alert :			Alert events related to the query
History:
2017-09-28 ericfe created
**********************************************************************************************/
WITH running_q AS (
select decode(trim(q.service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Periscope_monitoring', '12', 'Default', trim(q.service_class)) as "queue", 
m.blocks_read read_mb, decode(m.blocks_to_disk,-1,null,m.blocks_to_disk) spill_mb
from  stv_wlm_query_state q 
left outer join stv_query_metrics m on ( q.query = m.query and m.segment=-1 and m.step=-1 )
order by q.service_class,q.exec_time desc, q.wlm_start_time)
SELECT queue, sum(read_mb) as mem_mb, sum(spill_mb) as spill_mem_mb
FROM running_q
GROUP BY queue
HAVING mem_mb > 0