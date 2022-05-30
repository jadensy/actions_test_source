select
  date_trunc,
  card,
  count(*)
from [cards_generated_by_date]
where [date_trunc=daterange]
group by 1,2
union all
select
  [timestamp:date] as date_trunc,
  'Cohort Size' as card,
  count(*)
from prod.card_status 
where card_status = 'generated'
and [timestamp=daterange]
group by 1,2