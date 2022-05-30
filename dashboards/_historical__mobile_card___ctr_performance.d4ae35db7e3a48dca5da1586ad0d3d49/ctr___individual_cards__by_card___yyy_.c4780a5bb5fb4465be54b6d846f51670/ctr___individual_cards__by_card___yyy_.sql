select card,
       "YYY",
       viewed,
       engaged,
       engaged/ nullif(Cast(viewed as float), 0) as "CTR"
from
(
  select card, 
         Applied4Loan + '|' + cm + '|' + verified "YYY",
         sum(is_viewed) viewed,
         sum(is_engaged) engaged
  from
  (
    select users.user_id email,
           card,
           is_generated,
           is_viewed,
           is_engaged,
            Case when Applied4Loan is null then 'N' else Applied4Loan end Applied4Loan,
            Case when verified is null then 'N' else verified end verified,
            Case when cm is null then 'N' else cm end cm
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
      --where [timestamp=daterange]
    ) as users
    left join
    (select distinct email, 'Y' Applied4Loan
      from
        (
        select distinct email from ml_finance.fpall_ll
        union
        select distinct email from ml_finance.fpall_ml
        )
    ) applicants -- Loan Applicants
    on users.user_id = applicants.email
    left join
    (select distinct user_id, 'Y' verified
      from prod.bank_verification_status
      where bank_verification_status = 'linked'
        -- and link_source = 'mobileapp'
    ) BVs -- Bank Verification
    on users.user_id = BVs.user_id
    left join
    (select
      distinct user_id, 'Y' cm
      from prod.enrollment_status
      where enrollment_status = 'success'
        -- and (creditmonitoring_source = 'ios' or creditmonitoring_source = 'android')
    ) CM_enrolled -- Enrolled In Credit Monitoring 
    on users.user_id = CM_enrolled.user_id
  )
  group by 1, 2
)
where card != ''
order by 2 desc, 1