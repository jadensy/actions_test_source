--no_cache
with t1 as (
select
  a.call_id,
  [created_at:week] as report_week,
  wait_duration,
  a.status
from ujetapi.call as a 
left join ujetapi.call_menu_path as b on a.call_id = b.call_id
where status in ('failed','finished','recovered')
and deflection not in ('after_hours_message_only','temp_redirection_phone')
--and call_type = 'IvrCall'
and a.call_id not in (select call_id from ujetapi.call where status in ('failed','recovered') and wait_duration < 10)
and b.name like '%Metabank%'

),

t2 as (
select
  count(case when wait_duration < 600 and status != 'failed' and report_week < current_date - 6 and report_week > current_date - 12 then call_id end) as sub5_lw,
  count(case when report_week < current_date - 6 and report_week > current_date - 12 then call_id end) as tot_lw,
  count(case when wait_duration < 600 and status != 'failed' and report_week < current_date - 13 and report_week > current_date - 19 then call_id end) as sub5_pw,
  count(case when report_week < current_date - 13 and report_week > current_date - 19 then call_id end) as tot_pw,
  ((count(case when wait_duration < 600 and status != 'failed' and report_week < current_date - 6 and report_week > current_date - 32 then call_id end)) * 1.0) / 4 as sub5_4wk,
  ((count(case when report_week < current_date - 6 and report_week > current_date - 32 then call_id end)) * 1.0) / 4 as tot_4wk
from t1
)

SELECT 
  sub5_lw * 1.0 / tot_lw as "SLA Last Week",
  ((sub5_lw * 1.0 / tot_lw) - (sub5_pw * 1.0 / tot_pw)) * 1.0 / (sub5_pw * 1.0 / tot_pw) as "% Change from Prev Week",
  sub5_pw * 1.0 / tot_pw as "SLA Prev Week",
  ((sub5_lw * 1.0 / tot_lw) - (sub5_4wk * 1.0 / tot_4wk)) * 1.0 / (sub5_4wk * 1.0 / tot_4wk) as "% Change from 4wk Avg",
  sub5_4wk *1.0 / tot_4wk as "Last 4 Week Avg"
  
FROM t2