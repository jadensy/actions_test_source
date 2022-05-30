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
),

base2 as (
select 
  count(case when ttfr <= 120 and report_week < current_date - 6 and report_week > current_date - 12 then id end) as sub_lw,
  count(case when missed = 'f' and report_week < current_date - 6 and report_week > current_date - 12 then id end) as tot_lw,
  count(case when ttfr <= 120 and report_week < current_date - 13 and report_week > current_date - 19 then id end) as sub_pw,
  count(case when missed = 'f' and report_week < current_date - 13 and report_week > current_date - 19 then id end) as tot_pw,
  ((count(case when ttfr <= 120 and report_week < current_date - 6 and report_week > current_date - 32 then id end)) * 1.0) / 4 as sub_4wk,
  ((count(case when missed = 'f' and report_week < current_date - 6 and report_week > current_date - 32 then id end)) * 1.0) / 4 as tot_4wk
from base1

)

select 
  sub_lw * 1.0 / tot_lw as "SLA Last Week",
  ((sub_lw * 1.0 / tot_lw) - (sub_pw * 1.0 / tot_pw)) * 1.0 / (sub_pw *1.0 / tot_pw) as "% Change from Prev Week",
  sub_pw * 1.0 / tot_pw as "SLA Prev Week",
  ((sub_lw * 1.0 / tot_lw) - (sub_4wk * 1.0 / tot_4wk)) * 1.0 / (sub_4wk * 1.0 / tot_4wk) as "% Change from 4wk Avg",
  sub_4wk * 1.0 / tot_4wk as "Last 4 Week Avg"

from base2