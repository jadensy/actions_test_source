--no_cache
/**********************************************************************************************/
select 
query,
decode(trim(service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Periscope_monitoring', '12', 'Default', trim(service_class)) as "queue", 
decode(trim(q.state), 'Running','Run','QueuedWaiting','Queue','Returning','Return',trim(q.state)) as state, 
wlm_start_time as start_time,
q.queue_Time/1000000 as q_sec, q.exec_time/1000000 as exe_sec
from  stv_wlm_query_state q 
order by queue, state