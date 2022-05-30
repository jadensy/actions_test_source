SELECT
  [attributes__created_at:month],
  complaints20_root_cause_str,
  count(*)
from operations.kustomer_conversations
where complaints20_product_cb_str = 'Credit Reporting'
and complaints20_product_str = 'Credit Builder Plus'
and [attributes__created_at:month] > current_date - 180
group by 1,2
order by 1 desc