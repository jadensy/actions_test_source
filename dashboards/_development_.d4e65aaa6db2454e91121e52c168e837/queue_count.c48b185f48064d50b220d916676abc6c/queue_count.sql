--no_cache
/**********************************************************************************************/
select 
decode(trim(service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Periscope_monitoring', '12', 'Default', trim(service_class)) as "queue", 
decode(trim(q.state), 'Running','Run','QueuedWaiting','Queue','Returning','Return',trim(q.state)) as state, COUNT(q.state) as "count"
from  stv_wlm_query_state q 
group by queue, state
order by queue, state