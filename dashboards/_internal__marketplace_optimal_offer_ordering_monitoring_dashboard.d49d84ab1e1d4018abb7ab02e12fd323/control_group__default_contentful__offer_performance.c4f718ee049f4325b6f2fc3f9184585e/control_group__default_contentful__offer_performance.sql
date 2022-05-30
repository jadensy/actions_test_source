with exp_user as (
select 
  user_id, 
  assignment_group, 
  injection_time
--   min(injection_time) as first_injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
-- group by 1, 2
)

, engagement as (
  select
      partner,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click,
      count(distinct case when event = 'view' then email else null end) as uniq_view,
      count(distinct case when event = 'click' then email else null end) as uniq_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user 
  on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.injection_time
  where event_timestamp >= '2022-01-07' and event_timestamp <= '2022-01-26'
  and partner!='LoanInATapCard'
  and medium <> 'dashboard_web'
  and assignment_group= '-1'
  group by 1
  order by 1
)

, conversion as (
  select
      CASE WHEN fct.partner_service_name = '5k funds 74' THEN '5KFunds'
           WHEN fct.partner_service_name = 'Rapid5k funds' THEN 'Fast Funds Finally'
           WHEN fct.partner_service_name LIKE '%(3901) Degree Locate%' THEN 'EduLGD'
           WHEN fct.partner_service_name = 'Loanupp ML V2' 
             and campaign_id != '1OFlxwDvVZdFgsLlm2yeVz' THEN 'Did you still need cash?'
           WHEN fct.partner_service_name = 'Loanupp ML V2' 
             and campaign_id = '1OFlxwDvVZdFgsLlm2yeVz' THEN 'Do you need access to a loan now?'
           WHEN fct.partner_service_name = 'Fiona loans (MarketPlace)' THEN 'Fiona'
           WHEN fct.partner_service_name = 'Insurify In App' THEN 'Insurify'
           WHEN fct.partner_service_name = 'Visible ML' THEN 'Visible'
           WHEN fct.partner_service_name = 'Savvy ML' THEN 'Savvy'
           WHEN fct.partner_service_name = 'Dovly' THEN 'Dovly' 
           WHEN fct.partner_service_name = 'Divvy Home Marketplace' THEN 'Rent to Own Your Home'
           WHEN fct.partner_service_name LIKE '%Lexington%' THEN 'Repair your credit score'
           WHEN fct.partner_service_name = 'Ethos ML' THEN 'Life Insurance'
           WHEN fct.partner_service_name = '6031 | CPL - Auto Credit Express (app marketplace)' THEN 'Auto Credit Express'
      ELSE fct.partner_service_name END AS partner,
      fct.partner_service_name,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    --round(sum(revenue)::float,2) as total_rev
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_rev
  from marketplace.fct_conversion_revenue as fct
  join exp_user 
  on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
where clicked_at >= '2022-01-07' and clicked_at <= '2022-01-26'
  and medium = 'marketplace'
  and assignment_group= '-1'
  group by 1,2
)

select engagement.partner,
  conversion.partner_service_name,
  engagement.total_view,
  engagement.uniq_view,
  engagement.total_click,
  engagement.uniq_click,
  coalesce(conversion.total_conv, 0) as total_conv,
  coalesce(conversion.uniq_conv, 0) as uniq_conv,
  coalesce(conversion.total_rev, 0) as total_rev,
  engagement.partner as "Partner",
  conversion.partner_service_name as "Partner Service Name",
  round((total_view::decimal / uniq_view),4) as "view/user",
  round((total_click::decimal / uniq_view),4) as "click/user",
  round((total_conv::decimal / uniq_view),4) as "conv/user",
  round((total_rev::decimal / uniq_view),4) as "rev/user",
  round((total_click::decimal / total_view),4)  as "click/view",
  round((total_conv::decimal / NULLIF(total_click,0)),4) as "conv/click",
  round((1000*total_rev::decimal / total_view),4) as rpm,
  round((total_rev::decimal / NULLIF(total_click,0)),4) as "rev/click",
  round((total_rev::decimal / NULLIF(total_conv, 0)),4) as "rev/conv"
from engagement 
left join conversion on engagement.partner = conversion.partner
order by 4 DESC