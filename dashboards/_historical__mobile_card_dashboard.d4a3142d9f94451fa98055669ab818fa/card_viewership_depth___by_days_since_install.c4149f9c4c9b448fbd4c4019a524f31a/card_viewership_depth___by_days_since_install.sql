with installs as (
  select user_id as email, [min(timestamp):day] as install_date
  from prod.screen_viewed
  group by 1
  order by 2
),

sessions as
(
  select diff, day, user_id, total_cards + 1 as total_cards,
         isnull(depth, 0) + isnull(viewed, 0) as depth
  from
  (
    select normal_cards.*, Case when viewed is null then 0 else viewed end as viewed
    from
    (
      select diff, user_id, day,
             count(distinct position) as total_cards,
             count(distinct case when is_viewed = 1 then position end) as depth
      from
      (
        select user_id, position, is_viewed, email, install_date, [timestamp:day] as day, [timestamp:day] - install_date as diff
        from
        (
          select *
          from [card_data]
          order by user_id, timestamp desc
        ) as card_data
        left join
        installs
        on installs.email = card_data.user_id
        where [timestamp:day] >= [getdate():day] - 14
          and install_date >= [getdate():day] - 28
        order by 6, 7, 1, 2
      )
      group by 1, 2, 3
      order by 1, 2, 3 desc
    ) as normal_cards
    left join
    (
      select distinct user_id, [timestamp:day] as day, 1 as viewed
      from prod.trivia_status
      where trivia_status = 'viewed unlocked trivia'
    ) trivia_cards
    on normal_cards.user_id = trivia_cards.user_id
    and normal_cards.day = trivia_cards.day
  )
)

select diff as days_since_install,
       avg(total_cards) average_cards_generated,
       avg(depth) as average_depth,
       avg(depth::float / total_cards) as average_depth_percent,
       count(distinct user_id) as users
from sessions
where diff <= 14 and diff >= 0
group by 1
order by 1