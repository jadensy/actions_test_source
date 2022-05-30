select sum(clicked_cards)::float/sum(viewed_cards)
from
(
  select user_id,
          [timestamp:day] as day,
          is_viewed as viewed_cards,
          is_engaged as clicked_cards,
          row_number() over(Partition by user_id order by timestamp) as rn
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
  where card = 'PayOffAllCreditCardsCmTipsCard  ' and is_viewed = 1
  order by 1, 2 desc
)
where rn = 14