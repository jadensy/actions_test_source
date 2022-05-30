select [users.timestamp:day] as Week, sum(is_viewed) as card_viewed, sum(is_engaged) card_clicked,
       count(viewed) as MLP_viewed, count(qualified) as MLP_qualified, count(subscribed) as MLP_subscribed 
from 
(
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
) users
-------------------------------------------------------------------------
left join
(
  select user_id viewed, min(timestamp) as viewed_date
  from prod.screen_viewed
  where screen = 'PlusIntro'
    and user_id not like '%moneylion%'
  group by 1
) as v
on users.user_id = v.viewed
and users.timestamp < v.viewed_date
and [users.timestamp:day] = [v.viewed_date:day]
-------------------------------------------------------------------------
left join
(
  select user_id qualified, min(timestamp) as qualified_date
  from prod.screen_viewed
  where screen = 'PlusInvestmentQuestions'
  and user_id not like '%moneylion%'
  group by 1
) as q
on users.user_id = q.qualified
and users.timestamp < q.qualified_date
and [users.timestamp:day] = [q.qualified_date:day]
-------------------------------------------------------------------------
left join
(
  select user_id subscribed, min(timestamp) as subscribed_date
  from prod.screen_viewed
  where screen = 'PlusLoanOffer'
  and user_id not like '%moneylion%'
  group by 1
) as s
on users.user_id = s.subscribed
and users.timestamp < s.subscribed_date
and [users.timestamp:day] = [s.subscribed_date:day]
-------------------------------------------------------------------------
where card in ('DirectingUsersToPlusCard' /*,
                'PlusEduHowWeInvestCard',
                'PlusEduInvestMoreSaveMoreCard',
                'PlusEduRewardPointsCard',
                'PlusEduSaveMoreLevelUpCard',
                'PlusEduUsePlusLoanCard',
                'PlusEligibleLowBalanceCard',
                'PlusEligibleLowSavingsCard',
                'PlusLoanDepositedAlertCard' */
              )
group by 1
order by 1 desc