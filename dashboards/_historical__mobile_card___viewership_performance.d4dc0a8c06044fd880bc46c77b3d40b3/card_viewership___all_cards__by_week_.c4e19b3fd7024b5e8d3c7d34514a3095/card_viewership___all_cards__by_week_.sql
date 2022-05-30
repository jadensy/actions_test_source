select [timestamp:week] as week, sum(is_viewed) as viewed_cards
from
(
  select first as card, first_viewed as is_viewed, 'first' as whereis, timestamp
  from [card_performance]

  union all

  select second as card, second_viewed as is_viewed, 'second' as whereis, timestamp
  from [card_performance]

  union all

  select third as card, third_viewed as is_viewed, 'third' as whereis, timestamp
  from [card_performance]

  union all

  select fourth as card, fourth_viewed as is_viewed, 'fourth' as whereis, timestamp
  from [card_performance]
)
group by 1
order by 1 desc