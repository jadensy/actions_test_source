with cohort_size as
  (select
    [date_trunc:date] as date_trunc,
    card,
    count(*) as c
  from [cards_viewed_by_date]
  where [date_trunc=daterange]
  group by 1,2)

select
  date_trunc,
  card,
  votes / views as relative,
  votes,
  views
from
  (select
    t.date_trunc,
    t.card,
    count(*) as votes,
    cohort_size.c::float as views
  from
    (select * 
    from [cards_markedhelpful_by_date]
    union all
    select * 
    from [cards_markednothelpful_by_date]) as t
  full join cohort_size on 
    t.date_trunc = cohort_size.date_trunc
    and t.card = cohort_size.card
  where [t.date_trunc=daterange]
  group by 1,2,4)