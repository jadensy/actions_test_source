select card,
        sum(is_generated) as generated,
        sum(is_viewed) as viewed,
        sum(is_engaged) as engaged,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
[card_data]
left join
usr_rsulca.cards_master_list
on card_data.card = cards_master_list.key

where card != ''
  and [timestamp=daterange]
  and sub_category = 'BV'
group by 1
order by 1