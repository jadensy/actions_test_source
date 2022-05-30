with cm_cards as (
  select distinct key
  from usr_rsulca.cards_master_list
  where sub_category = 'CM'
)

select [timestamp:day] AS day, count(distinct card)
from [card_data]
where card in (select * from cm_cards)
group by 1
order by 1 desc