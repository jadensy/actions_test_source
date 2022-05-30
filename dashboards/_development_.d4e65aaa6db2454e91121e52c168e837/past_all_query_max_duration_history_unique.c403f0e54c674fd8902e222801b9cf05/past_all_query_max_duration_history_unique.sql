--no-cache
with ctx as (
    select 
        MAX(DATEDIFF('second', starttime, endtime)) as duration
    from admin.stl_query_history
    where starttime >= DATE_TRUNC('day',sysdate) - 7
    group by querytxt
)
select 
    count(*), 
    CASE
    WHEN duration < 7200 THEN duration
    ELSE '7200'
    END as duration2
from ctx
where duration > 10
group by duration2
order by duration2