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

, engagement as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      engagement.partner,
      mapping.conversion_partner_name as conversion_partner_name,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click,
      count(distinct case when event = 'view' then email else null end) as uniq_view,
      count(distinct case when event = 'click' then email else null end) as uniq_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user 
  on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.first_injection_time
  left join marketplace_static.partner_name_mapping as mapping 
  on engagement.partner = mapping.engagement_partner_title and engagement.event_timestamp between mapping.mapping_start_time and   mapping.mapping_end_time
  where event_timestamp >= '2022-01-03' and event_timestamp < '2022-01-18'
  and engagement.partner!='LoanInATapCard'
  and medium <> 'dashboard_web'
  group by 1,2,3
)

, conversion as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      fct.partner_service_name AS conversion_partner_name,
    count(fct.userid) as total_conv,
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

select engagement.assignment_group_name,
  engagement.partner,
  conversion.conversion_partner_name,
  engagement.uniq_view,
  engagement.total_view,
  round((coalesce(total_rev,0)::decimal / uniq_view),4) as "rev/user"
from engagement 
left join conversion on engagement.conversion_partner_name = conversion.conversion_partner_name and engagement.assignment_group_name = conversion.assignment_group_name
order by 4 DESC