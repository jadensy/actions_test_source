--no_cache
/*
The purpose of the view admin.v_check_wlm_query_time is to get  WLM Queue Wait Time , Execution Time and Total Time by Query for the past 7 Days 
*/
select * from admin.v_check_wlm_query_time
where queue_start_time >= dateadd(day,-1,CURRENT_DATE)
order by exec_seconds desc, total_seconds desc
limit 100