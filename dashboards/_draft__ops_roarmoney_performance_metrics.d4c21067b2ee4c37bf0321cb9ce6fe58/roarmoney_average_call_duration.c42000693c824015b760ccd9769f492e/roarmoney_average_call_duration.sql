--no_cache
with normal_call as (
select 
    a.call_id, 
    date(a.created_at) as contact_date, 
    a.call_duration, 
    b.name
from ujetapi.call as a
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
where a.call_type = 'IvrCall'
and a.status in ('finished','recovered')
and date(a.created_at) >= current_date - 365
and a.call_duration > 10
and a.connected_at is not null
and b.name like '%Metabank%'

)

SELECT 
  [contact_date:week] as reporting_week, 
  AVG(call_duration) * 1.0 / 60 as "Average Call Duration"
from normal_call
where reporting_week > current_date - 60
and reporting_week < current_date - 5
GROUP BY 1
ORDER BY 1 DESC