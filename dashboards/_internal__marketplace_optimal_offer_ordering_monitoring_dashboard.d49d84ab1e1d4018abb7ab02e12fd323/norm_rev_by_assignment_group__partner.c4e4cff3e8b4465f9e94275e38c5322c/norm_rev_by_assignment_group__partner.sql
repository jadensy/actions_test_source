--monitoring
--by group
--overall
with exp_user as (
select 
  user_id, 
  assignment_group, 
  injection_time
--   min(injection_time) as first_injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
-- group by 1, 2
)

, conversion as (
select  
	  userid,
    assignment_group,
    fct.partner_service_name,
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_norm_rev
from marketplace.fct_conversion_revenue as fct
join exp_user 
on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
where clicked_at >= '2022-01-07' and clicked_at <= '2022-01-26'
and medium = 'marketplace'
group by 1, 2, 3
)

select 
  partner_service_name,
  (case when conversion.assignment_group = '-1' then 'control (default Contentful)'
     when conversion.assignment_group = '0' then 'control-control (AIDE inject)'
     when conversion.assignment_group = '1' then 'treatment group' end) as assignment_group_name,
  count(userid) as total_conv,
  count(distinct userid) as uniq_conv,
  round(sum(coalesce(total_norm_rev,0))::float,2) as total_norm_rev
from exp_user 
left join conversion on exp_user.user_id = conversion.userid
group by 1, 2
order by total_norm_rev desc