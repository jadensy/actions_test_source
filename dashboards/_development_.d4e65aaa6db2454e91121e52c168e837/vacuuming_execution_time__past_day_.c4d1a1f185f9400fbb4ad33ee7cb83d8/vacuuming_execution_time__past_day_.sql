--no_cache
select * from (
  SELECT w.query,
       SUBSTRING(q.querytxt,1,200) AS querytxt,
       w.queue_start_time,
       decode(trim(w.service_class), '6', 'Long_queries', '7', 'Etls', '8', 'Analytics', '9', 'Periscope', '10', 'Segment', '11', 'Default', trim(w.service_class)) as "queue", 
--        w.service_class AS class,
--        w.slot_count AS slots,
       w.total_queue_time / 1000000 AS queue_seconds,
       w.total_exec_time / 1000000 exec_seconds,
       (w.total_queue_time + w.total_exec_time) / 1000000 AS total_seconds
  FROM stl_wlm_query w
  LEFT JOIN stl_query q
        ON q.query = w.query
        AND q.userid = w.userid
  WHERE w.queue_start_time >= DATEADD (day,-7,CURRENT_DATE)
        AND   w.total_queue_time > 0
        AND   w.userid > 1
        AND   q.starttime >= DATEADD (day,-7,CURRENT_DATE)
--   ORDER BY w.total_queue_time DESC,
--            w.queue_start_time DESC
)
where queue_start_time >= dateadd(day,-1,CURRENT_DATE)
and querytxt ~ '.*Vacuum.*'
order by queue_start_time