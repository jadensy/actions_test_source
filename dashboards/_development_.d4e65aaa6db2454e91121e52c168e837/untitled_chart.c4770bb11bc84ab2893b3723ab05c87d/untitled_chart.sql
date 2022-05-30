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
select trim(u.usename) as user, s.pid,q.query, decode(trim(q.service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Periscope_monitoring', '12', 'Default', trim(q.service_class)) as "queue", 
date_trunc('second',q.wlm_start_time) as start,decode(trim(q.state), 'Running','Run','QueuedWaiting','Queue','Returning','Return',trim(q.state)) as state, 
q.queue_Time/1000000 as q_sec, q.exec_time/1000000 as exe_sec
from  stv_wlm_query_state q 
left join stl_querytext s on (s.query=q.query and sequence = 0)
left join pg_user u on ( s.userid = u.usesysid )