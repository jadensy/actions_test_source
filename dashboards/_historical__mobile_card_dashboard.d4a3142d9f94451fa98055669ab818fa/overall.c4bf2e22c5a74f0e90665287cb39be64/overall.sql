select card, sum(is_generated) as generated, sum(is_viewed) as viewed, sum(is_engaged) as clicked, sum(is_engaged)::float / nullif(sum(is_viewed), 0) as CTR
from
(
  select card_data.*
  from [card_data]
  where card != ''
)
group by 1
order by 2 desc