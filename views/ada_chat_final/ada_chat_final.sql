--no_cache
-- Removing duplicates
with convo as
(select row_number() over (partition by conversation_id order by source_start_date) as rn, *
from operations.ada_chat_history)

,mlemployee as
(select distinct userid
  from ml_public.subscription
  where subtype = 'MLemployee'

  union

  select DISTINCT _id
  from lion1.user
  where email like '%@moneylion.com')

,deduped as
(select *
from convo 
where rn = 1
and   meta_userid not in (select * from mlemployee)
and   lower(global_email) not like '%test.com')

  select 
  conversation_id,
  conversation_date,
  conversation_ended,
  conversation_duration_in_minutes,
  conversation_url,
  csat_score,
  csat_resolved,
  csat_comment,
  agent_id,
  has_handoff,
  case when message like '%do not navigate away from this%' then True 
            else False end as queued_livechat,
  
  case when has_handoff is False and agent_id is null and queued_livechat is False then 'chatbot'
        when has_handoff is True and agent_id is null and queued_livechat is True then 'request_agent_dropoff'
        when has_handoff is True and agent_id is not null and queued_livechat is True then 'livechat'
        when message like '%we suggest that you call the MoneyLion Support team%Please call us at the number below%' then 'long_queue_call'
        when message like '%we suggest that you call the MoneyLion Support team%' and message not like '%Ok, we are connecting you to our support team%' then 'long_queue_dropoff'
        when has_handoff is True and agent_id is null and queued_livechat is False then 'redirected'
              else 'error' end as chat_type,
  
  case when datepart(dw, conversation_date) in (1, 2, 3, 4, 5) and datepart(hour, conversation_date) between 8 and 23 then true
       when datepart(dw, conversation_date) in (6) and datepart(hour, conversation_date) between 10 and 19 then true
             else false end as during_livechat_hours,
  
  global_product as global_product_v1,
  global_productsecondary,
  global_email,
  global_currentqueuelength,
  global_chatter_id_timestamp,
  meta_userid,
  meta_email,
  meta_source,
  meta_device,
  meta_csat_score,
  meta_test_user,
  meta_last_question_asked,
  positive_responses,
  negative_responses,
  last_response,
  trim(response_2_before_last) + ';' 
  + trim(response_before_last) + ';'
  + trim(last_response) as chatbot_responses,
  typed_messages,
  case when message like '%past two weeks%' then 'true' 
            else 'false' end as rechat_l2weeks,
  row_number() over (partition by meta_userid order by conversation_date) as user_chat_cnt,
  case when user_chat_cnt = 1 then 'new_chatter' else 'returning_chatter' end as chatter_type,
  source_attachment, 
  source_start_date,
  source_end_date,
  global_groups_concatenated
  from deduped