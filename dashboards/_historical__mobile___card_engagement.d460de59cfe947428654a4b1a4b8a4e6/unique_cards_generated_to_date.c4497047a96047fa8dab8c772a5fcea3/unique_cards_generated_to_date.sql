select count(distinct card)
from [cards_generated_by_date]
where date_trunc >= '2017-05-25'