--by partner
with exp_user as (
select user_id, assignment_group , min(injection_time) as first_injection_time
from marketplace_static.exp_orm_v1b_20211217 as exp
group by 1,2 
)

,partner_rev as (
select partner_service_name, revenue_per_conversion
from marketplace_static.partner_revenue_per_conversion 
where month = '2021-11-01'
)

, visitor as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      count(distinct case when event = 'view' then email else null end) as user_count
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user
  on engagement.userid  = exp_user.user_id 
  and engagement.event_timestamp >= exp_user.first_injection_time and engagement.event_timestamp < '2022-01-18'
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  group by 1
)

, engagement as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      partner,
      offer_index,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click,
      count(distinct case when event = 'view' then email else null end) as uniq_view,
      count(distinct case when event = 'click' then email else null end) as uniq_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user
  on engagement.userid  = exp_user.user_id 
  and engagement.event_timestamp >= exp_user.first_injection_time and engagement.event_timestamp < '2022-01-18'
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  and partner = 'Life Insurance'
  group by 1,2,3
)

select engagement.*, 
  round(uniq_click::decimal/uniq_view,4) as "unique_click/user",
  round(total_click::decimal/uniq_view,4) as "click/user",
  round(total_click::decimal/total_view,4) as "click/view",
  round(uniq_click::decimal/user_count,4) as "unique_click/user(visitor)"
from engagement
join visitor 
on engagement.assignment_group_name = visitor.assignment_group_name
where partner not in ('FutureFuel.io','Be prepared when your job\'s not there')
and engagement.assignment_group_name != 'control-control (AIDE inject)'
order by engagement.assignment_group_name, partner, offer_index