--no-cache
select 
    count(*), 
    CASE
    WHEN DATEDIFF('second', starttime, endtime) < 60 THEN DATEDIFF('second', starttime, endtime)
    ELSE '60'
    END as duration
from admin.stl_query_history
where querytxt like '%Query generated by Periscope Data%'
and querytxt not like '%"query_source":"app_supercache"%'
and querytxt not like '%"user_email":%'
and starttime >= DATE_TRUNC('day',sysdate) - 7
group by duration
order by duration