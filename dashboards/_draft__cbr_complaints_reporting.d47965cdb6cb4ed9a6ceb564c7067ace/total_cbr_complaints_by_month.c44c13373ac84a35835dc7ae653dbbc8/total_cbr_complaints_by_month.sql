SELECT
  [attributes__created_at:month],
  count(case when complaints20_complaint_type_str = 'Non Regulatory' then id end) as non_reg,
  count(case when complaints20_complaint_type_str = 'Regulatory' then id end) as reg,
  count(*) as total
from operations.kustomer_conversations
where complaints20_product_cb_str = 'Credit Reporting'
and complaints20_product_str = 'Credit Builder Plus'
and [attributes__created_at:month] > current_date - 180
group by 1
order by 1 desc