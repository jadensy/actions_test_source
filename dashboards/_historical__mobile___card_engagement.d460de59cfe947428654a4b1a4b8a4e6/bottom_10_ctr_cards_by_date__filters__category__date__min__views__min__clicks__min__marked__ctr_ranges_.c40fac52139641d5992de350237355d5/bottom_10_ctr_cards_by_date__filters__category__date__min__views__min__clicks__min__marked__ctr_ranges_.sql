with clicks as
  (select
    card,
    count(*) as clicks
  from [cards_engaged_by_date]
  where [date_trunc=daterange]
  group by 1),

views as
  (select
    card,
    count(*) as views
  from [cards_viewed_by_date]
  where [date_trunc=daterange]
  group by 1),

helpful as
  (select
    card,
    count(*) as c
  from [cards_markedhelpful_by_date]
  where [date_trunc=daterange]
  group by 1),

not_helpful as
  (select
    card,
    count(*) as c
  from [cards_markednothelpful_by_date]
  where [date_trunc=daterange]
  group by 1),

generated as
  (select
    card,
    count(*) as generated
  from [cards_generated_by_date]
  where [date_trunc=daterange]
  group by 1)

select *
from
(
select
  views.card,
  [card_category],
  isnull(generated.generated, 0) as generated,
  isnull(views.views, 0) as views,
  isnull(clicks.clicks, 0) as clicks,
  isnull((isnull(helpful.c,0) + isnull(not_helpful.c,0)),0) as marked,
  isnull(views.views / generated.generated::decimal, 0) as vtr,
  isnull(clicks.clicks / views.views::decimal, 0) as ctr,
  isnull(cast((isnull(helpful.c,0) + isnull(not_helpful.c,0)) as decimal) / clicks.clicks, 0) as mtr,
  isnull(helpful.c, 0) as helpful,
  isnull(not_helpful.c, 0) as not_helpful,
  case 
    when isnull((isnull(helpful.c,0) - isnull(not_helpful.c,0)),0) > 0
      then '+' || isnull((isnull(helpful.c,0) - isnull(not_helpful.c,0)),0)::text
      else isnull((isnull(helpful.c,0) - isnull(not_helpful.c,0)),0)::text
  end as net_helpful_score,
  isnull(cast(helpful.c as decimal) / (clicks.clicks), 0) as helpful_over_clicked,
  isnull(cast(helpful.c as decimal) / ((isnull(helpful.c,0) + isnull(not_helpful.c,0))), 0) as helpful_over_marked
from views
left join generated on views.card = generated.card
left join clicks on views.card = clicks.card
left join helpful on views.card = helpful.card
left join not_helpful on views.card = not_helpful.card
where clicks.clicks / views.views::decimal * 100 between [Low_Range_CTR_PCT] and [High_Range_CTR_PCT]
and [views.views>=Minimum_Views]
and [clicks.clicks>=Minimum_Clicks]
and [(isnull(helpful.c,0) + isnull(not_helpful.c,0))>=Minimum_Marked]
)
where [category=Card_Category]
order by ctr
limit 10