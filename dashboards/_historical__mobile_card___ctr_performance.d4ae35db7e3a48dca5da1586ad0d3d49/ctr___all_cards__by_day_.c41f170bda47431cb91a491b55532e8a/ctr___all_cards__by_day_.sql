select [timestamp:day] as day, 
        sum(is_viewed) as is_viewed,
        sum(is_engaged) as clicked_cards,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
(
  select distinct card, is_viewed, is_engaged, user_id, timestamp
  from
  (
    select first as card, first_viewed as is_viewed, first_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select second as card, second_viewed as is_viewed, second_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select third as card, third_viewed as is_viewed, third_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select fourth as card, fourth_viewed as is_viewed, fourth_engaged as is_engaged, user_id, timestamp
    from [card_performance]
  )
)
group by 1
order by 1 desc