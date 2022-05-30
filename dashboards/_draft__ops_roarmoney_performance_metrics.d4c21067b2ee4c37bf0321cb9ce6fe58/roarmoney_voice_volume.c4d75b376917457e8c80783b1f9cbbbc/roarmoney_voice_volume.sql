--no_cache
with RM_call as (
select 
  a.call_id, 
  date(a.created_at) as contact_date,
  a.connected_at
from ujetapi.call as a 
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
--where call_type = 'IvrCall'
--where status != 'deflected'
where b.name like '%Metabank%'
and date(created_at) >= current_date - 100 
)

select 
  [contact_date:week] as reporting_week, 
  count(call_id) as "Calls Queued",
  count(case when connected_at is not null then call_id end) as "Calls Handled"
from RM_call
where reporting_week > current_date - 60
and reporting_week < current_date - 5
group by 1
order by 1 desc