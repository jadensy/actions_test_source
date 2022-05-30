select distinct card, sub_category, sum(is_generated) as generated
from [card_data]
left join
usr_rsulca.cards_master_list
on card_data.card = cards_master_list.key
where card != ''
  and category is null
  and sub_category is null
group by 1, 2
order by 3 desc