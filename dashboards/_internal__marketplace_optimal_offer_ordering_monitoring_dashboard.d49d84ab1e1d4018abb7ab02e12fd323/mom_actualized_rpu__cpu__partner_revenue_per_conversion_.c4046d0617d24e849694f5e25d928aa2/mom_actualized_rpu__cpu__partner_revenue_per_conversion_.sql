-- select distinct month 
-- from marketplace_static.partner_revenue_per_conversion
-- order by 1

-- select *
-- from marketplace_static.partner_revenue_per_conversion
-- where month = '2021-12-01'
-- limit 20

with partner_rev as (
  select DATE_TRUNC('month', month) as conversion_month,
    sum(conversion) as mth_conversion, 
    sum(unique_conversion) as mth_unique_conversion, 
    sum(total_rev_usd) mth_total_rev_usd, 
    sum(total_rev_usd)/sum(conversion) as mth_revenue_per_conversion
  from marketplace_static.partner_revenue_per_conversion
  group by 1
  order by 1
  )

, engagement as (
  select
    DATE_TRUNC('month', event_timestamp) as engagement_month,
    count(distinct case when event = 'view' then email else null end) as mth_unique_view,
    count(distinct case when event = 'click' then email else null end) as mth_unique_click
  from marketplace.fct_organic_engagement_events
  where event_timestamp >= '2021-01-01' and event_timestamp <= '2022-01-09'
  group by 1
)

, final_table as (
  select 
    partner_rev.conversion_month, 
--     mth_conversion, mth_unique_conversion, mth_revenue_per_conversion,
    mth_unique_view, mth_unique_click, 
    mth_total_rev_usd, 
    round((mth_total_rev_usd::decimal / mth_unique_view),4) as rpu,
    round((mth_unique_click::decimal / mth_unique_view),4) as cpu
  from partner_rev
  left join engagement
  on partner_rev.conversion_month = engagement.engagement_month
  order by 1
)

select * from final_table