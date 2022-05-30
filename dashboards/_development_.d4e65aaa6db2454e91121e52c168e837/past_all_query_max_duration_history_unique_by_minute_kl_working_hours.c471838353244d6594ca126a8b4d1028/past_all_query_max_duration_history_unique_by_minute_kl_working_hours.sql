--no-cache
with ctx as (
    select 
        MAX(DATEDIFF('minute', starttime, endtime)) as duration
    from "admin".stl_query_history
    where 1=1
and EXTRACT(HOUR FROM starttime) >=1
  and EXTRACT(HOUR FROM starttime) <=11
    AND userid <> 114
	AND userid <> 233
	AND userid <> 169
  AND LOWER(querytxt) LIKE '%select%'
    group by querytxt
)
SELECT
	count(1)
  ,CASE 
 when duration >=0 and duration < 60 then duration
  ELSE 61
  END AS duration_for_grouping
-- 	, duration AS duration_for_grouping
FROM ctx
GROUP BY duration_for_grouping
ORDER BY duration_for_grouping