--no_cache
with RM_call as (
select 
  a.call_id, 
  [created_at:week] as report_week,
  a.connected_at
from ujetapi.call as a 
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
--where call_type = 'IvrCall'
--where status != 'deflected'
where b.name like '%Metabank%'
and date(created_at) >= current_date - 100 
),

base as (
select 
  '1' as "RN",
  'Calls Queued' as "Channel",
  count(case when report_week < current_date - 6 and report_week > current_date - 12 then call_id end) as last_week,
  count(case when report_week < current_date - 13 and report_week > current_date - 19 then call_id end) as prev_week,
  ((count(case when report_week < current_date - 6 and report_week > current_date - 32 then call_id end)) * 1.0) / 4 as four_week
from RM_call

union all
  
select 
  '2' as "RN",
  'Calls Handled' as "Channel",
  count(case when connected_at is not null and report_week < current_date - 6 and report_week > current_date - 12 then call_id end) as last_week,
  count(case when connected_at is not null and report_week < current_date - 13 and report_week > current_date - 19 then call_id end) as prev_week,
  ((count(case when connected_at is not null and report_week < current_date - 6 and report_week > current_date - 32 then call_id end)) * 1.0) / 4 as four_week
from RM_call
  
)

SELECT 
  RN,
  Channel,
  last_week,
  (last_week - prev_week) * 1.0 / NULLIF(prev_week,0) as "% Change from Prev Week",
  prev_week,
  (last_week - four_week) * 1.0 / NULLIF(prev_week,0) as "% Change from 4wk Avg",
  four_week as "Last 4 Week Avg"
from base
order by 1