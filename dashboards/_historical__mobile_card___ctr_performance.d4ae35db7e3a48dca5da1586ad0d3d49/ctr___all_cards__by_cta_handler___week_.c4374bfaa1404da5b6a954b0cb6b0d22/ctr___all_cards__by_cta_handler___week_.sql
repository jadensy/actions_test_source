/* select cta_handler, [timestamp:week] as week,
        sum(is_viewed) as viewed,
        sum(is_engaged) as engaged,
        isnull( Cast(sum(is_engaged) as float) / 
               NULLIF(sum(is_viewed), 0), 0) as CTR
from
(
  select card,
         ctaHandler as temp,
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
         is_viewed, is_engaged, timestamp 
  from
  (
    select first as card, first_viewed as is_viewed, first_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select second as card, second_viewed as is_viewed, second_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select third as card, third_viewed as is_viewed, third_engaged as is_engaged, timestamp
    from [card_performance]

    union all

    select fourth as card, fourth_viewed as is_viewed, fourth_engaged as is_engaged, timestamp
    from [card_performance]
  ) as card_data
  
  left join
  
  usr_rsulca.cards_master_list
  on card_data.card = cards_master_list.key
)
group by 1, 2
order by 1 NULLS LAST, 2 desc */