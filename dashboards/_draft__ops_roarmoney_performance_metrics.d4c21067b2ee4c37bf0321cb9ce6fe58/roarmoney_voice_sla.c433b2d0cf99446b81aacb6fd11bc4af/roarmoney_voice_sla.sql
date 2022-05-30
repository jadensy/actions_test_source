--no_cache
with t1 as (
select
  [created_at:week] as reporting_week,
  count(case when wait_duration < 600 and status != 'failed' then a.call_id end) as sub5,
  count(a.call_id) as tot
from ujetapi.call as a 
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
where status in ('failed','finished','recovered')
and deflection not in ('after_hours_message_only','temp_redirection_phone')
--and call_type = 'IvrCall'
and a.call_id not in (select call_id from ujetapi.call where status in ('failed','recovered') and wait_duration < 10)
and b.name like '%Metabank%'
group by 1
order by 1
)

SELECT 
  reporting_week,
  (sub5 *1.0 / tot) as service_level
FROM t1
where reporting_week < current_date - 1
and reporting_week > current_date - 60



-- Very accurrate compared to "Weekly Metrics" figures, outside of week of 6/29