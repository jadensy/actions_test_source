-- Complete Query before Randomizing
with initial_query as
(
  -- Plus Users
  select email, day, cards, install_date
  from
  (
    select distinct email
    from
    (
      select
        distinct userid
      from ml_public.subscription
      where activeon >= '2018-01-15'
      and doc_type = 'ML-Plus'
      and subtype != 'ReturningOOS'
    ) plusers
    left join
    (
      select userid, email
      from ml_public.usercollection
    ) user_info
    on plusers.userid = user_info.userid
  ) plus_users
  left join
  (
    -- Card Data on Users
    select [timestamp:day] as day, cards, user_id,
           row_number() over(partition by user_id order by timestamp) as rn
    from prod.card_status
    where card_status = 'generated'
  ) card_data
  on plus_users.email = card_data.user_id
  
  left join
  (
    select user_id, min(timestamp) as install_date
    from prod.screen_viewed
    group by 1
  ) min_installs
  on plus_users.email = min_installs.user_id 

  where rn <= 20 -- First 7 generated Cards
  order by email, day
),

-- Random sample of 10 Users
random_sample as
(
  select TOP 10 email
  from
  (
    select email, substring(random()::varchar, 9, 4)::int as randy
    from
    (
      select distinct email
      from initial_query
      where install_date >= '2018-01-15'
    )
  )
  order by randy desc
)

-- Final Selection
select *
from initial_query
where email in (select * from random_sample)
order by email