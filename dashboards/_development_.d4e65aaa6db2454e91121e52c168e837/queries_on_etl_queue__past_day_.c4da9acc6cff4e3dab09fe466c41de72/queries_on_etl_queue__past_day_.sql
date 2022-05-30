--no_cache

select query, querytxt, queue_start_time, queue_seconds, exec_seconds, total_seconds
from admin.v_check_wlm_query_time
where queue_start_time >= dateadd(day,-1,CURRENT_DATE)
and class = 7
and total_seconds > 0
order by queue_start_time desc