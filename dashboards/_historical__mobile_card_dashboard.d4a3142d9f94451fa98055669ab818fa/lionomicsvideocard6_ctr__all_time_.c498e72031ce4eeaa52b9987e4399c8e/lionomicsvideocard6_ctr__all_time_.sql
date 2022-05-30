select clicks::float / views as CTR
from
(
  select
    sum(is_generated) as generations,
    sum(is_viewed) as views,
    sum(is_engaged) as clicks
  from [card_data]
  where card = 'LionomicsVideoCard6'
--   group by 1
--   order by 1 desc
)