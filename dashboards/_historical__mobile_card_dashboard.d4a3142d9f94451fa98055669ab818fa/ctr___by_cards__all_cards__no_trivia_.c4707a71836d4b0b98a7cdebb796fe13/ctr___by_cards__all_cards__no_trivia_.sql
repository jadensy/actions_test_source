select card,
        sum(is_generated) as generated,
        sum(is_viewed) as viewed,
        sum(is_engaged) as engaged,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
[card_data]
where card != ''
  and [timestamp=daterange]
group by 1
order by 1