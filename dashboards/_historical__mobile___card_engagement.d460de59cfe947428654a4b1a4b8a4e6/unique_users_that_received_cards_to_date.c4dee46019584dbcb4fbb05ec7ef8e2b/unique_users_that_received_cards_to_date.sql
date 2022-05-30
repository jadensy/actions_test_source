select count(distinct user_id)
from prod.card_status
[without ML user_ids]
and timestamp >= '2017-05-25'