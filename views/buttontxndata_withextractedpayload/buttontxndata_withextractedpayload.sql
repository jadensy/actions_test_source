--no_cache
select 
  bt_txn_main.*
, bt_txn_pldata.*
from reward.buttononlinedealtransactions bt_txn_main
left join reward.buttononlinedealtransactions_payload bt_txn_payload 
  on bt_txn_main.buttononlinedealtransactions_id = bt_txn_payload.buttononlinedealtransactions_id
left join reward.buttononlinedealtransactions_payload_data bt_txn_pldata
  on bt_txn_payload.buttononlinedealtransactions_payload_id = bt_txn_pldata.buttononlinedealtransactions_payload_id

/*
, f_dict_to_json(payload) as vjson
, json_extract_path_text(vjson, 'data', 'button_order_id', true) as button_order_id
, json_extract_path_text(vjson, 'data', 'currency', true) as currency
, json_extract_path_text(vjson, 'data', 'order_currency', true) as order_currency
, json_extract_path_text(vjson, 'data', 'order_total', true) as order_total
, json_extract_path_text(vjson, 'data', 'amount', true) as commission_amount
, json_extract_path_text(vjson, 'data', 'order_purchase_date', true) as order_purchase_date_epoch
, json_extract_path_text(vjson, 'data', 'event_date', true) as event_date_epoch
, timestamp 'epoch' + cast(event_date_epoch AS bigint)/1000 * interval '1 second' AS event_date
*/