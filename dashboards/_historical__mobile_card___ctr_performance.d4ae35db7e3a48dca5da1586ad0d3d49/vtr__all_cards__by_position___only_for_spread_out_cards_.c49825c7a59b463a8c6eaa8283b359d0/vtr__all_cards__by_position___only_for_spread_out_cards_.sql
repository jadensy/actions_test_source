with spread_out_cards as (
  select distinct card
  from (
    select card, count(distinct position) as locations,
           avg(generated) avg_generated, stddev(generated) as sd_generated,
           avg(viewed) avg_viewed, stddev(viewed) as sd_viewed,
           avg(clicked) avg_clicked, stddev(clicked) as sd_clicked
    from (
      select card, position, count(1) as generated, sum(is_viewed) as viewed, sum(is_engaged) as clicked
      from
      (
        select first as card, 1 as position, first_viewed as is_viewed, first_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select second as card, 2 as position, second_viewed as is_viewed, second_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select third as card, 3 as position, third_viewed as is_viewed, third_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select fourth as card, 4 as position, fourth_viewed as is_viewed, fourth_engaged as is_engaged, user_id, timestamp
        from [card_performance]
      )
      where card != ''
      group by 1, 2
      order by 1, 2
    )
    group by 1
    order by 1
  )
  where locations = 4
    and sd_generated < avg_generated / 2
)

select position,
       sum(generated) as generated,
       sum(viewed) viewed, sum(viewed)::float / sum(generated) as vtr,
       sum(clicked) clicked, sum(clicked)::float / sum(viewed) as ctr
from (
  select card, position, count(1) as generated,
         sum(is_viewed) as viewed,
         sum(is_engaged) as clicked
  from
  (
    select first as card, 1 as position, first_viewed as is_viewed, first_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select second as card, 2 as position, second_viewed as is_viewed, second_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select third as card, 3 as position, third_viewed as is_viewed, third_engaged as is_engaged, user_id, timestamp
    from [card_performance]

    union all

    select fourth as card, 4 as position, fourth_viewed as is_viewed, fourth_engaged as is_engaged, user_id, timestamp
    from [card_performance]
  )
  where card in (select * from spread_out_cards)
  group by 1, 2
  order by 1, 2
)
group by 1
order by 1