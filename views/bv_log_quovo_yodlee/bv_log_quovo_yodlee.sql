select created_on,
updated_on, 
event_name,
status,
provider_inst_id,
b.institution_name,
code,
source,
api_code,
api_message, 
client_id,
provider, 
resp,
json_extract_path_text(resp, 'statusMessage') as resp_statusMessage,
json_extract_path_text(resp, 'code')as resp_code,
json_extract_path_text(resp, 'message') as resp_message,
json_extract_path_text(resp, 'display') as resp_display,
json_extract_path_text(resp, 'apiCode') as resp_apiCode,
json_extract_path_text(resp, 'apiMessage') as resp_apiMessage
from ml_public.mysql_bv_log as a 
  left join [bv_institutions as b]
  on (a.provider_inst_id = b.provider_id and a.provider = b.provider_name)
where provider in ('quovo','yodlee')
and b.provider_name in ('quovo','yodlee')
and created_on >= '2018-08-02'