select week,
       cm,
       viewed,
       engaged,
       engaged/ nullif(Cast(viewed as float), 0) as "CTR"
from
(
  select week, 
         cm,
         sum(is_viewed) viewed,
         sum(is_engaged) engaged
  from
  (
    select users.user_id email,
           [timestamp:week] as week,
           card,
           is_generated,
           is_viewed,
           is_engaged,
            Case when cm is null then 'Not Enrolled in CM' else cm end cm
    from
    ( -- Users who have viewed a card
      select card, user_id,
             is_viewed, is_engaged, is_generated, timestamp 
      from
      (
        select first as card, 1 is_generated, first_viewed as is_viewed, first_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select second as card, 1 is_generated, second_viewed as is_viewed, second_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select third as card, 1 is_generated, third_viewed as is_viewed, third_engaged as is_engaged, user_id, timestamp
        from [card_performance]

        union all

        select fourth as card, 1 is_generated, fourth_viewed as is_viewed, fourth_engaged as is_engaged, user_id, timestamp
        from [card_performance]
      )
    ) as users
    left join
    (select
      distinct user_id, 'Enrolled in CM' cm
      from prod.enrollment_status
      where enrollment_status = 'success'
        -- and (creditmonitoring_source = 'ios' or creditmonitoring_source = 'android')
    ) CM_enrolled -- Enrolled In Credit Monitoring 
    on users.user_id = CM_enrolled.user_id
  )
  where card != ''
  group by 1, 2
)
order by 2, 1 desc

/* NOTE: The Timestamp is for the Card Performance, while the binning (Cm enrolled, etc) is for their current status regardless of timestamps. */