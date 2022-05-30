select
  [created_at:week] as report_week,
  INITCAP(replace(replace(replace(complaints_2_0_product_overtime, '_', ' '), '34', ''),'2', '')) as "Prod: BNPL",
  count(*)
from zendesk.tickets
where complaints_2_0_complaint_type in ('non_regulatory','regulatory')
and complaints_2_0_product = 'overtime__buy_now_pay_later_'
and report_week > current_date - 45
and report_week < current_date - 5
group by 1,2