select cta_handler,
       "YYY",
       viewed,
       engaged,
       engaged/ nullif(Cast(viewed as float), 0) as "CTR"
from
(
  select cta_handler, 
         Applied4Loan + '|' + cm + '|' + verified "YYY",
         sum(is_viewed) viewed,
         sum(is_engaged) engaged
  from
  (
    select card,
           users.user_id,
           CASE when ctaHandler = 'route:LoanList' or ctaHandler = 'loanapp' then 'Loan Related'
                when ctaHandler = 'transactions' then 'Transactions (Spending, Overdraft, New Funds)'
                when ctaHandler = 'route:CreditCards' then 'Credit Card Usage & Tips'
                when ctaHandler = 'route:CreditMonitoringContainer' then 'Credit Score & Credit Monitoring Tips'
                when ctaHandler = 'route:GroupedInstitutions' then 'Adding/Linking Account/Card'
                when ctaHandler = 'route:CommunityAndInvites' then 'Invites and Boost'
                when ctaHandler like '%blog%' then 'Blog'
                when (ctaHandler like '%.moneylion%') or (ctaHandler like '%/moneylion%')
                or (ctaHandler= '') or (ctaHandler is null) or ctaHandler in ('phone', 'providers', 
                                                                              'route:Institutions', 'route:RewardsContainer') 
                    then 'Other'
                else '3rd Party Ad' end
           as cta_handler,
           is_viewed, is_engaged,
           Case when Applied4Loan is null then 'N' else Applied4Loan end Applied4Loan,
           Case when verified is null then 'N' else verified end verified,
           Case when cm is null then 'N' else cm end cm,
           timestamp 
    from
    (
      select first as card, first_viewed as is_viewed, first_engaged as is_engaged, user_id, timestamp
      from [card_performance]

      union all

      select second as card, second_viewed as is_viewed, second_engaged as is_engaged, user_id, timestamp
      from [card_performance]

      union all

      select third as card, third_viewed as is_viewed, third_engaged as is_engaged, user_id, timestamp
      from [card_performance]

      union all

      select fourth as card, fourth_viewed as is_viewed, fourth_engaged as is_engaged, user_id, timestamp
      from [card_performance]
    ) as users

    left join

    usr_rsulca.cards_master_list
    on users.card = cards_master_list.key
    
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
order by 2 desc, 1