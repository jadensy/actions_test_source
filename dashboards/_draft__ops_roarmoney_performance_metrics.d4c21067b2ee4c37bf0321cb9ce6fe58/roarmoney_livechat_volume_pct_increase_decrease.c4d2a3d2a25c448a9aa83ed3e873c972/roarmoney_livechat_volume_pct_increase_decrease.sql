with base1 AS 
(
SELECT 
    zendesk_ticket_id as id, 
    [session_start_date:week] as report_week, 
    response_time_first as ttfr, 
    missed, 
    session_end_date
from operations.zendesk_chat_chats
where session_start_date > current_date - 100
and zendesk_ticket_id in (select ticket_id from zendesk.ticket_events where ticket_form_id = '360000735292')
and tags like '%roarmoney%'
and missed = 'f'
),

base2 as (
select 
  'Livechat Volume' as "Metric",
  count(case when report_week < current_date - 6 and report_week > current_date - 12 then id end) as last_week,
  count(case when report_week < current_date - 13 and report_week > current_date - 19 then id end) as prev_week,
  ((count(case when report_week < current_date - 6 and report_week > current_date - 32 then id end)) *1.0) / 4 as four_week
from base1
)

select 
  Metric,
  last_week,
  ((last_week - prev_week) *1.0) / NULLIF(prev_week,0) as "% Change from Prev Week",
  prev_week,
  ((last_week - four_week) *1.0) / NULLIF(prev_week,0) as "% Change from 4wk Avg",
  four_week as "Four Week Avg"
from base2