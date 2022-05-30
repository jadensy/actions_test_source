with pfmkt_users as (
  select *
  from (
    select 
      email,
      userid,
      new_core_channel,
      new_core_campaign_type,
      new_core_SU_dtime,
      lite_channel,
      lite_campaign_type,
      lite_su_dtime
    
   from marketing.fct_pfmkt_user_view_ffa 
    where 1=1
  )
)
select [new_core_SU_dtime:month] as reporting_month,
count(distinct case when new_core_SU_dtime is not null then email end) as total_paid_core,
count(distinct case when new_core_channel = 'facebook' then email end) as fb_core,
count(distinct case when new_core_channel = 'facebook' and new_core_campaign_type = 'web campaign' then email end) as fb_core_web,
count(distinct case when new_core_channel = 'facebook' and new_core_campaign_type = 'app campaign' then email end) as fb_core_app,
count(distinct case when new_core_channel = 'google' then email end) as google_core,
count(distinct case when new_core_channel = 'google' and new_core_campaign_type = 'web campaign' then email end) as google_core_web,
count(distinct case when new_core_channel = 'google' and new_core_campaign_type = 'app campaign' then email end) as google_core_app,
count(distinct case when new_core_channel = 'apple' then email end) as apple_core,
count(distinct case when new_core_channel = 'snapchat' then email end) as snapchat_core,
count(distinct case when new_core_channel = 'bing' then email end) as bing_core,
count(distinct case when new_core_channel = 'reddit' then email end) as reddit_core,
count(distinct case when new_core_channel = 'liftoff' then email end) as liftoff_core
from pfmkt_users
where reporting_month >= '2020-09-01'
group by 1
order by 1 desc