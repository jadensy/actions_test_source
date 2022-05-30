SELECT
  id as "ZD Ticket ID",  
  created_at,
  status,
  ticket_source,
  disposition_2_0_bnpl,
  description

from zendesk.tickets
where disposition_2_0_product = 'bnpl_-_buy_now_pay_later'
--and (ticket_source in ('web_form', 'email','mobile_sdk') or ticket_source is null)
--and session_type is null
--and description not like 'Chat started%'
--and description != 'X'
--and created_at >= current_date - 10