select [timestamp:day] as day,
       sum(is_generated) as generated,
       sum(is_viewed) as viewed,
       sum(is_engaged) as clicked,
       isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
[card_data]
left join
usr_rsulca.cards_master_list
on card_data.card = cards_master_list.key

where card != ''
  and [timestamp=daterange]
  and (sub_category = 'BLOG' or sub_category='LIONOMICS')
group by 1
order by 1 desc