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

, engagement AS
(
select 
	assignment_group,
    count(case when event = 'view' then 1 else null end) as total_view,
    count(case when event = 'click' then 1 else null end) as total_click,
    count(distinct case when event = 'view' then user_id else null end) as uniq_view,
    count(distinct case when event = 'click' then user_id else null end) as uniq_click
from marketplace.fct_organic_engagement_events as engagement
join exp_user 
on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.injection_time
where event_timestamp >= '2022-01-07' and event_timestamp <= '2022-01-26'
  and partner!= 'LoanInATapCard'
  and medium != 'dashboard_web'
group by 1
)

, conversion as
(select  
	  assignment_group,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    round(sum(revenue)::float,2) as total_rev,
--     round(sum(case when revenue_per_conversion is null then revenue else revenue_per_conversion end)::float,2) as total_rev
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_norm_rev
from marketplace.fct_conversion_revenue as fct
join exp_user 
on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
where clicked_at >= '2022-01-07' and clicked_at <= '2022-01-26'
and medium = 'marketplace'
group by 1
)

, pivoted as
(select 
  (case when engagement.assignment_group = '-1' then 'control (default Contentful)'
       when engagement.assignment_group = '0' then 'control-control (AIDE inject)'
       when engagement.assignment_group = '1' then 'treatment group' end) as assignment_group_name,
  engagement.total_view,
  engagement.uniq_view
from engagement 
left join conversion on engagement.assignment_group = conversion.assignment_group
order by 2 desc
limit 10
)

-- select * from pivoted

-- SELECT pivoted.assignment_group_name, unpiv.col_name, unpiv.col_value
-- FROM pivoted
-- CROSS JOIN LATERAL (VALUES(pivoted.total_view),(pivoted.uniq_view)) as unpiv(col_name, col_value)

SELECT LATERAL (VALUES(pivoted.total_view),(pivoted.uniq_view))
FROM pivoted
-- CROSS JOIN LATERAL (VALUES(pivoted.total_view),(pivoted.uniq_view)) as unpiv(col_name, col_value)

-- select t.name, c.category_mix, t.tag
-- from table1 t
--    cross join lateral ( 
--        values (category1), (category2), (category3)
--    ) as c(category_mix);

-- select c.assignment_group_name, t.*
-- from pivoted as c
--   cross join lateral (
--      values 
--        (total_view, 'total_view'),
--        (uniq_view, 'uniq_view')
--   ) as t(total_view, uniq_view)