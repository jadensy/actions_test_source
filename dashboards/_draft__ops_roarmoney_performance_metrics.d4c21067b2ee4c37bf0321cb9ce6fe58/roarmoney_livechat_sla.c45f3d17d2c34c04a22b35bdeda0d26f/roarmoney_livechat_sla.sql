with base1 AS 
(
SELECT zendesk_ticket_id as id, date(session_start_date) as contact_date, response_time_first as ttfr, missed, session_end_date
from operations.zendesk_chat_chats
where session_start_date > current_date - 100
and zendesk_ticket_id in (select ticket_id from zendesk.ticket_events where ticket_form_id = '360000735292')
and tags like '%roarmoney%'
),

base2 as (
select 
  [contact_date:week] as reporting_week, 
  --contact_date, 
  count(case when ttfr <= 120 then id end) as sub,
  count(case when missed = 'f' then id end) as tot
from base1
group by 1
order by 1 desc
)

select 
  --contact_date,
  reporting_week, 
  sub * 1.0 / tot as sla
from base2
where reporting_week < current_date - 5
and reporting_week > current_date - 60

order by 1 desc