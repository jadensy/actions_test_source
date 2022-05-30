--no-cache
with ctx as (
    select 
        MAX(DATEDIFF('second', starttime, endtime)) as duration
    from admin.stl_query_history
    where starttime >= DATE_TRUNC('day',sysdate) - 7
    and userid not in (1, 121, 103, 107, 102, 124, 136, 144, 169, 176, 175, 212, 222, 234, 233, 114, 242, 254, 110)
    group by querytxt
)
select 
    count(*), 
    CASE
    WHEN duration < 120 THEN duration
    ELSE '120'
    END as duration2
from ctx
group by duration2
order by duration2