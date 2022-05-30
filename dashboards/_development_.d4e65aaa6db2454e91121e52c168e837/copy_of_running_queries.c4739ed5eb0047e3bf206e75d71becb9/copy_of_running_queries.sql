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
q.queue_Time/1000000 as q_sec, q.exec_time/1000000 as exe_sec,
-- substring(replace(trim(translate(s.text,chr(10)||chr(13)||chr(9) ,'')),'\\n',' '),1,90) as sql_statement
-- substring(replace(trim(translate(s.text,chr(10)||chr(13)||chr(9) ,'')),'\\n',' '),1,90) as sql
substring(s.querytxt,1,200) as sql
from  stv_wlm_query_state q 
left join stl_query s on (s.query=q.query)
-- left outer join svl_statementtext s on (s.xid=q.xid and sequence = 0)
left outer join pg_user u on ( s.userid = u.usesysid )
-- LEFT OUTER JOIN (SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
--                    FROM stl_utilitytext ut
--                    WHERE sequence = 0
--                    AND upper(TEXT) like 'DECLARE%'
--                    GROUP BY text, ut.xid) qrytext_cur ON (q.xid = qrytext_cur.xid)
order by q.service_class,q.exec_time desc, q.wlm_start_time