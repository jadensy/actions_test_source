select [timestamp:week] as week, 
        sum(is_viewed) as viewed_cards,
        sum(is_engaged) as clicked_cards,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
(
  select first as card, first_viewed as is_viewed, first_engaged as is_engaged, timestamp
  from [card_performance]
)
group by 1
order by 1 desc