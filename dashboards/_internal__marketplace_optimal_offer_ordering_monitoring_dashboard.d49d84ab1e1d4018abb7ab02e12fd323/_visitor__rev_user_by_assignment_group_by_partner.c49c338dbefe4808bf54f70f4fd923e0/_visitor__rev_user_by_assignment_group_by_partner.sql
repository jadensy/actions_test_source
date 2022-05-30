--by partner
with exp_user as (
select 
  user_id, 
  assignment_group, 
  min(injection_time) as first_injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
group by 1, 2
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
  and engagement.event_timestamp >= exp_user.first_injection_time and engagement.event_timestamp <= '2022-01-21'
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  group by 1
)

, engagement as (
  select
  	  partner,
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      --offer_index,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click,
      count(distinct case when event = 'view' then email else null end) as uniq_view,
      count(distinct case when event = 'click' then email else null end) as uniq_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user
  on engagement.userid  = exp_user.user_id 
  --and engagement.event_timestamp >= exp_user.first_injection_time
  and engagement.event_timestamp >= '2022-01-07' and engagement.event_timestamp <= '2022-01-21'
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  group by 1,2
  order by 1,2,3 desc
)

, conversion as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      CASE WHEN fct.partner_service_name = '5k funds 74' THEN '5KFunds'
           WHEN fct.partner_service_name = 'Rapid5k funds' THEN 'Fiona'
           WHEN fct.partner_service_name LIKE '%(3901) Degree Locate%' THEN 'EduLGD'
           WHEN fct.partner_service_name = 'Loanupp ML V2' THEN 'Did you still need cash?'
           WHEN fct.partner_service_name = 'Family-Life-Coverage' THEN 'Life Insurance'
           WHEN fct.partner_service_name = 'Fiona loans (MarketPlace)' THEN 'Fiona'
           WHEN fct.partner_service_name = 'Insurify In App' THEN 'Insurify'
           WHEN fct.partner_service_name = 'Visible ML' THEN 'Visible'
           WHEN fct.partner_service_name = 'Savvy ML' THEN 'Savvy'
           WHEN fct.partner_service_name ='Dovly' THEN 'Dovly' 
           WHEN fct.partner_service_name LIKE '%Divvy Home Marketplace%' THEN 'Divvy Home Marketplace'
           WHEN fct.partner_service_name LIKE '%Lexington%' THEN 'Repair your credit score'
      ELSE fct.partner_service_name END AS partner,
      fct.partner_service_name,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    --round(sum(revenue)::float,2) as total_rev
    round(sum(coalesce(revenue_per_conversion,0))::float,2) as total_rev
  from marketplace.fct_conversion_revenue as fct
  join exp_user 
  on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.first_injection_time
  left join partner_rev
  on fct.partner_service_name = partner_rev.partner_service_name
  where clicked_at >= '2022-01-07' and clicked_at <= '2022-01-21'
  and medium = 'marketplace'
  group by 1,2,3
)

select engagement.assignment_group_name,
  engagement.partner,
  conversion.partner_service_name,
  engagement.uniq_view,
  engagement.total_view,
  --round((total_rev::decimal / uniq_view),4) as "rev/user",
  round(total_rev::decimal/user_count,4) as "rev/user"
from engagement
join visitor 
on engagement.assignment_group_name = visitor.assignment_group_name
left join conversion on engagement.partner = conversion.partner and engagement.assignment_group_name = conversion.assignment_group_name
order by 4 DESC