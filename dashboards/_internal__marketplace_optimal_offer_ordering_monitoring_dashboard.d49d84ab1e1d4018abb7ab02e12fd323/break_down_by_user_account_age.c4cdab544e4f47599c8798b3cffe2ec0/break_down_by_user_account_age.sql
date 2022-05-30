--monitoring
--by group
--overall
with exp_user as (
select 
  user_id, 
  assignment_group, 
  injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
)

, user_account_age as (
select 
  _id as userid,
  (case when datediff(day, createdon, injection_time) > 30 then '> 30 days' else '< 30 days' end) as account_age_bucket
from lion1.user as lion1
join exp_user 
on exp_user.user_id = lion1._id
) 

, engagement AS (
select 
  account_age_bucket,
    count(case when event = 'view' then 1 else null end) as total_view,
    count(case when event = 'click' then 1 else null end) as total_click,
    count(distinct case when event = 'view' then user_id else null end) as uniq_view,
    count(distinct case when event = 'click' then user_id else null end) as uniq_click
from marketplace.fct_organic_engagement_events as engagement
join exp_user on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.injection_time
left join user_account_age on engagement.userid  = user_account_age.userid
where partner!= 'LoanInATapCard'
  and medium != 'dashboard_web'
group by 1
)

, conversion as
(select  
    account_age_bucket,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_norm_rev,
    round(sum(revenue)::float,2) as total_rev
--     round(sum(case when revenue_per_conversion is null then revenue else revenue_per_conversion end)::float,2) as total_rev
from marketplace.fct_conversion_revenue as fct
join exp_user 
on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
left join user_account_age on fct.userid  = user_account_age.userid
where clicked_at >= '2022-01-07' and clicked_at <= '2022-01-26'
 and medium = 'marketplace'
group by 1
)

select 
  engagement.account_age_bucket,
  engagement.total_view,
  engagement.uniq_view,
  engagement.total_click,
  engagement.uniq_click,
  coalesce(conversion.total_conv, 0) as total_conv,
  coalesce(conversion.uniq_conv, 0) as uniq_conv,
  coalesce(conversion.total_norm_rev, 0) as total_norm_rev,
  coalesce(conversion.total_rev, 0) as total_rev,

  round((total_norm_rev::decimal / NULLIF(uniq_view, 0)),4) as "norm_rev/user",
  round((total_rev::decimal / NULLIF(uniq_view, 0)),4) as "rev/user",
  round((total_click::decimal / NULLIF(uniq_view, 0)),4) as "click/user",

  round((total_view::decimal / NULLIF(uniq_view, 0)),4) as "view/user",
  round((total_conv::decimal / NULLIF(uniq_view, 0)),4) as "conv/user",
  round((total_click::decimal / NULLIF(total_view, 0)),4)  as "click/view",
  round((total_conv::decimal / NULLIF(total_click, 0)),4) as "conv/click",

  round((total_norm_rev::decimal / NULLIF(total_click, 0)),4) as "norm_rev/click",
  round((total_norm_rev::decimal / NULLIF(total_conv, 0)),4) as "norm_rev/conv",
  round((1000*total_norm_rev::decimal / NULLIF(total_view, 0)),4) as "norm_rev/mil",

  round((total_rev::decimal / NULLIF(total_click, 0)),4) as "rev/click",
  round((total_rev::decimal / NULLIF(total_conv, 0)),4) as "rev/conv",
  round((1000*total_rev::decimal / NULLIF(total_view, 0)),4) as "rev/mil"
from engagement 
left join conversion on engagement.account_age_bucket = conversion.account_age_bucket
order by 2