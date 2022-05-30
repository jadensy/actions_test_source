-- no_cache
-- need to etl this to observe trend as we optimize
select "hour", trunc(hour) AS "day",                                                      
decode(trim(service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Periscope_monitoring', '12', 'Default', trim(service_class)) as "queue",
query_count, total_queue_time_sum, total_exec_time_sum, percent_wlm_queue_time
from admin.v_check_wlm_query_trend_hourly
where service_class >=5
and service_class <=11
and service_class != 10
and "hour" >= '2019-04-10'