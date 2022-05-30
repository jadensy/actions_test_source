select 
  id,
  ticket_number_str,
  attributes__created_at as created_at,
  complaints20_complaint_type_str,
  complaints20_product_str,
  complaints20_product_cb_str,
  complaints20_root_cause_str,
  complaints20_error_fault_str,
  complaints20_channel_str,
  complaints20_complaint_descr_str
from operations.kustomer_conversations
where complaints20_product_cb_str = 'Credit Reporting'
and complaints20_product_str = 'Credit Builder Plus'
and attributes__created_at > current_date - 200