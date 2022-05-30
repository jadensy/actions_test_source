SELECT
  [attributes__created_at:month],
  complaints20_error_fault_str,
  count(*)
from operations.kustomer_conversations
where complaints20_product_cb_str = 'Credit Reporting'
and complaints20_product_str = 'Credit Builder Plus'
and complaints20_complaint_type_str = 'Non Regulatory'
and [attributes__created_at:month] > current_date - 180
group by 1,2
order by 1 desc