select [timestamp:week] as week,
       Applied4Loan + '|' + cm + '|' + verified "YYY",
       sum(fourth_viewed) fourth_viewed,
       sum(first_viewed) first_viewed,
       sum(fourth_viewed) / Cast(sum(first_viewed) as float) as "View Rate"
from
(
  select users.user_id,
         first_viewed,
         fourth_viewed,
         Case when Applied4Loan is null then 'N' else Applied4Loan end Applied4Loan,
         Case when verified is null then 'N' else verified end verified,
         Case when cm is null then 'N' else cm end cm,
         timestamp 
  from
  (
    select user_id, fourth_viewed, first_viewed, timestamp
    from [card_performance]
    where first_viewed = 1
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
order by 2 desc, 1 desc