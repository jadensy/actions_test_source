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

, engagement AS
(
select 
	assignment_group,
	case when engagement.partner in ('Fiona', 'Did you still need cash?', '5KFunds', 'Fast Funds Finally', 'EduLGD')
    then engagement.partner else 'other_partner' end as partner,
  case when mapping.conversion_partner_name in ('Fiona loans (MarketPlace)', 'Loanupp ML V2','5k funds 74', 'Rapid5k funds', '(3901) Degree Locate NEW - Lion Path (Private for SID 1905)')
    then mapping.conversion_partner_name else 'other_partner' end as conversion_partner_name,
  count(case when event = 'view' then 1 else null end) as total_view,
  count(case when event = 'click' then 1 else null end) as total_click,
  count(distinct case when event = 'view' then user_id else null end) as uniq_view,
  count(distinct case when event = 'click' then user_id else null end) as uniq_click
from marketplace.fct_organic_engagement_events as engagement
join exp_user 
on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.first_injection_time
left join marketplace_static.partner_name_mapping as mapping 
on engagement.partner = mapping.engagement_partner_title and engagement.event_timestamp between mapping.mapping_start_time and   mapping.mapping_end_time
where event_timestamp >= '2022-01-03' and event_timestamp < '2022-01-18'
  and engagement.partner!= 'LoanInATapCard'
  and medium != 'dashboard_web'
group by 1,2,3
)

, conversion as
(select
    assignment_group,
    case when fct.partner_service_name in ('Fiona loans (MarketPlace)','Loanupp ML V2','5k funds 74','Rapid5k funds','(3901) Degree Locate NEW - Lion Path (Private for SID 1905)')
      then fct.partner_service_name else 'other_partner' end as conversion_partner_name,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    count(distinct fct.userid) as uniq_conv,
    --round(sum(revenue)::float,2) as total_rev
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_rev
from marketplace.fct_conversion_revenue as fct
join exp_user 
on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.first_injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
where clicked_at >= '2022-01-03' and clicked_at < '2022-01-18'
and medium = 'marketplace'
group by 1,2
)

select 
  case when engagement.assignment_group = '-1' then 'c' when engagement.assignment_group = '1' then 't' end as assignment_group,
  engagement.partner,
  round((total_click::decimal / uniq_view),4) as "click_per_user",
  round((coalesce(total_rev,0)::decimal / uniq_view),4) as "rpu",
  round((coalesce(total_rev,0)::decimal / NULLIF(total_click,0)),4) as "revenue_per_click",
  round((total_click::decimal / NULLIF(total_view,0)),4) as "ctr",
  round((total_view::decimal / uniq_view),4) as "view_per_user"
from engagement 
left join conversion on engagement.assignment_group = conversion.assignment_group and engagement.conversion_partner_name = conversion.conversion_partner_name
where  engagement.assignment_group in ('-1','1')
order by 1,2