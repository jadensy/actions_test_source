SELECT
  [created_at:week] as week,
  INITCAP(replace(replace(replace(disposition_2_0_bnpl, '_', ' '), '34', ''),'2', ''))  as "Dispo, BNPL",
  count(*)
from zendesk.tickets
where disposition_2_0_product = 'bnpl_-_buy_now_pay_later'
and week > current_date - 45
and week < current_date - 5
group by 1,2
order by 1 desc