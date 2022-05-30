with first_open as (
  select
    anonymous_id as usr,
    [min(timestamp):date] as install_date
  from prod.screen_viewed
  where [valid_screen_views]
  group by 1
), days_apart as (
  select
    anonymous_id as usr,
    [first_open.install_date:date] as install_date,
    [timestamp:date] as login_date,
    ([timestamp:date] - [first_open.install_date:date]) as days
  from prod.screen_viewed
  join first_open on
    prod.screen_viewed.anonymous_id = first_open.usr
  where [valid_screen_views]
  group by 1,2,3
), buckets as (
  select
    usr,
    install_date,
  case  
    when days_apart.days = 0 then 0
    when days_apart.days between 1 and 7 then 1
    when days_apart.days between 8 and 14 then 2
    when days_apart.days between 15 and 21 then 3
    when days_apart.days between 22 and 28 then 4
    when days_apart.days between 29 and 35 then 5
    when days_apart.days between 36 and 42 then 6
    when days_apart.days between 43 and 49 then 7
    when days_apart.days between 50 and 56 then 8
    when days_apart.days between 57 and 63 then 9
    when days_apart.days between 64 and 70 then 10
    when days_apart.days between 71 and 77 then 11
    when days_apart.days between 78 and 84 then 12
    end as login_week
  from days_apart
  group by 1,2,3
), cohort_start as (
  select
    [install_date:week] as week_starting,
    count(*) as cohort_size
  from buckets
  where login_week = 0
  group by 1
), final as (
  select
    [install_date:week] as week_starting,
    ('week '||login_week) as week, 
    cohort_size,
    login_week,
    count(*) as week_size
  from buckets
  join cohort_start on
    [buckets.install_date:week] = cohort_start.week_starting
  where login_week != 0
    and [install_date:week] < [getdate():week]
  group by 1,2,3,4
)

select week_starting,
  week_size::float / cohort_size as retention
from final
where week = 'week 4'
and week_starting > [getdate():week] - interval '20 weeks'
and week_starting < [getdate():week] - interval '4 week'
order by week_starting desc