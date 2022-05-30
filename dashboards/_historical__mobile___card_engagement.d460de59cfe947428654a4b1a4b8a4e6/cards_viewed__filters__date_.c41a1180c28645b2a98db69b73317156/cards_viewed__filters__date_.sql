select
  date_trunc,
  card,
  count(*)
from [cards_viewed_by_date]
where [date_trunc=daterange]
group by 1,2