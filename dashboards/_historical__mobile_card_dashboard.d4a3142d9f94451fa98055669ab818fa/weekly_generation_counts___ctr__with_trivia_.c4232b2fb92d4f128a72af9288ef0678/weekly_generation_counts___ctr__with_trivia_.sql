with final_data as
(
  select
    day, user_id,
    total_cards + 1 as total_cards,
    view_depth + viewed as view_depth,
    click_depth + clicked as click_depth
  from
  (
    select
      normal_cards.*,
      Case when viewed is null then 0 else viewed end as viewed,
      Case when clicked is null then 0 else viewed end as clicked
    from
    (
      select [timestamp:day] as day, user_id,
             count(distinct position) as total_cards,
             count(DISTINCT case when is_viewed = 1 then position end) as view_depth,
             count(DISTINCT case when is_engaged = 1 then position end) as click_depth
      from
      (
        select
          *,
          sum(is_viewed) over(partition by user_id, timestamp) as viewed_cards,
          sum(is_engaged) over(partition by user_id, timestamp) as clicked_cards
        from [card_data]
        order by user_id, timestamp desc
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
    left join
    (
      select distinct user_id, [timestamp:day] as day, 1 as clicked
      from prod.trivia_status
      where trivia_status = 'clicked play'
    ) trivia_cards_clicked
    on normal_cards.user_id = trivia_cards_clicked.user_id
    and normal_cards.day = trivia_cards_clicked.day
  )
)

select [day:week] as week,
       sum(total_cards) as generated,
       sum(view_depth) as viewed,
       sum(click_depth) as engaged,
       isnull( Cast(sum(click_depth) as float) / 
            NULLIF(sum(view_depth), 0), 0) as CTR
from final_data
group by 1
order by 1 desc