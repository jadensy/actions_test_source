select card,
        sum(is_generated) as generated,
        sum(is_viewed) as viewed,
        sum(is_engaged) as engaged,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
(
  select card,
         is_viewed, is_engaged, is_generated, timestamp 
  from
  (
    select first as card, 1 is_generated, first_viewed as is_viewed, first_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select second as card, 1 is_generated, second_viewed as is_viewed, second_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select third as card, 1 is_generated, third_viewed as is_viewed, third_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select fourth as card, 1 is_generated, fourth_viewed as is_viewed, fourth_engaged as is_engaged, timestamp
    from [card_performance]
  )
)
where card != ''
group by 1
order by 1