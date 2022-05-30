with bv_cards as (
  select distinct key
  from usr_rsulca.cards_master_list
  where sub_category = 'BV'
)

select distinct [timestamp:day] AS day, card
from [card_data]
where card in (select * from bv_cards)
order by 1 desc, 2
LIMIT 10