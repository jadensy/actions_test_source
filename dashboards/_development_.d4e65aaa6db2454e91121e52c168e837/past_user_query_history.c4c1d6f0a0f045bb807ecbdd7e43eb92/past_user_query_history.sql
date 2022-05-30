--no-cache
select 
    count(*), 
    CASE
    WHEN DATEDIFF('second', starttime, endtime) < 60 THEN DATEDIFF('second', starttime, endtime)
    ELSE '60'
    END as duration
from admin.stl_query_history
where userid not in (1, 121, 103, 107, 102, 124, 136, 144, 169, 176, 175, 212, 222, 234, 233, 114, 242, 254, 110)
and starttime >= DATE_TRUNC('day',sysdate) - 7
group by duration
order by duration