--avg unique offer view per user
with exp_user as (
select user_id, assignment_group , min(injection_time) as first_injection_time
from marketplace_static.exp_orm_v1b_20211217 as exp
group by 1,2 
)


, engagement as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      --offer_index,
      user_id,  
      count(distinct case when event = 'view' then partner else null end) as unique_partner_view
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user
  on engagement.userid  = exp_user.user_id 
  and engagement.event_timestamp >= exp_user.first_injection_time and engagement.event_timestamp < '2022-01-18'
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  group by 1,2
)

select assignment_group_name, round(sum(unique_partner_view)::decimal/count(user_id),4) as avg_unique_partner_view_per_user
from engagement
group by 1
order by 1