with cohort_size as
  (select
    [timestamp:date] as date_trunc,
    'Cohort Size' as card,
    count(*) as c
  from prod.card_status 
  where card_status = 'generated'
  and [timestamp=daterange]
  group by 1,2)

select
  date_trunc,
  card,
  c / cohort as relative
from
  (select
    t.date_trunc,
    t.card,
    count(*) as c,
    cohort_size.c::float as cohort
  from
    (select * 
    from [cards_generated_by_date]) as t
  join cohort_size on 
    t.date_trunc = cohort_size.date_trunc
  where [t.date_trunc=daterange]
  group by 1,2,4)