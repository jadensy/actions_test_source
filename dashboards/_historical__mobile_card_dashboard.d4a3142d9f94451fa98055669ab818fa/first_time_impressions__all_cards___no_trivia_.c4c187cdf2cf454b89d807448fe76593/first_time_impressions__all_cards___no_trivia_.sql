select source, cards.*
from
(
  select card, sum(is_generated) as generated, sum(is_viewed) as viewed, sum(is_engaged) as clicked,
               sum(is_engaged)::float / nullif(sum(is_viewed), 0) as CTR
  from
  (
    select card_data.*, row_number() over(partition by user_id, card order by timestamp) as rn
    from [card_data]
    where card != ''
  )
  where rn = 1
  group by 1
) cards
left join usr_rsulca.cards_master_list
on cards.card = cards_master_list.key

order by 2