select day,
       count(distinct user_id) as users,
       avg(total_cards) average_cards_generated,
       avg(depth) as average_depth,
       avg(depth::float/total_cards) as average_depth_percent
from
(

  select day, user_id, total_cards + 1 as total_cards, depth + viewed as depth
  from
  (
    select normal_cards.*, Case when viewed is null then 0 else viewed end as viewed
    from
    (
      select [timestamp:day] as day, user_id,
             count(distinct position) as total_cards,
             count(DISTINCT case when is_viewed = 1 then position end) as depth
      from
      (
        select *
        from
        (
          select *, sum(is_viewed) over(partition by user_id, timestamp) as viewed_cards
          from [card_data]
          order by user_id, timestamp desc
        )
--         where viewed_cards > 0
      ) as card_data
      group by 1, 2
    ) normal_cards
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
where day >= '2018-04-04'

group by 1
order by 1 desc