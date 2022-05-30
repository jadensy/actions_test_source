select engagement.engagement_month,
-- min_date,
-- max_date,
unique_view,
unique_click,
total_rev,
round((total_rev::decimal / unique_view), 4) as RPU,
round((unique_click::decimal / unique_view), 4) as CPU
from
(select 
    DATE_TRUNC('month', event_timestamp) as engagement_month,
    min(date(event_timestamp)) as min_date,
	  max(date(event_timestamp)) as max_date,
    count(distinct case when event = 'view' then email else null end) as unique_view,
    count(distinct case when event = 'click' then email else null end) as unique_click
from marketplace.fct_organic_engagement_events foee
-- inner join (
--     select distinct user_id
--     from marketplace_static.exp_users 
--     where assignment_group <= 0) exp
--     on foee.userid = exp.user_id
where event_timestamp >= '2021-01-01' and event_timestamp <= '2022-01-09'
group by engagement_month) engagement
left join
(select  
    DATE_TRUNC('month', converted_at) as conversion_month,
    count(userid) as conversion,
    count(distinct userid) as unique_conversion,
    sum(revenue) as total_rev
from marketplace.fct_conversion_revenue fcr
-- inner join (
--     select distinct user_id
--     from marketplace_static.exp_users 
--     where assignment_group <= 0) exp
--     on fcr.userid = exp.user_id
where clicked_at >= '2021-01-01' and clicked_at <= '2022-01-09'
group by conversion_month
) conversion
on engagement.engagement_month = conversion.conversion_month
order by min_date