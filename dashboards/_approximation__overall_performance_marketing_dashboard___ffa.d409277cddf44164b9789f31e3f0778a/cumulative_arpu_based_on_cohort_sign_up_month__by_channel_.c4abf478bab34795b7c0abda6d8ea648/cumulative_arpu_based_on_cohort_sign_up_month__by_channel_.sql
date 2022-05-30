-- This dashboard/chart has been updated by DE to apply the DDA2 schema migration changes on 2020-11-01 03:18:20.170922 
-- For more info, see https://moneylion.atlassian.net/wiki/spaces/DE/pages/1809711844/DDA2+Schema+Migration+NOV+2020

with base as(
  select user_id, date_login, os_type
  from (
    select user_id,
    'iOS' AS os_type,
    min("timestamp") AS date_login
    from ios.screen_viewed
    where true
      and user_id is not NULL
      and screen is not NULL
      and "timestamp" >= date('2020-01-01')
    group by 1
    
    union all
    
    select user_id,
    'Android' AS os_type,
    min("timestamp") AS date_login
    FROM android.screen_viewed
    where true
      and user_id is not NULL
      and screen is not NULL
      and "timestamp" >= date('2020-01-01')
    group by 1
  )
),

ios_android_login as (
  
  select b1.user_id as email, b1.date_login as min_login_dtime, b1.os_type
  from base as b1
  join (
    select user_id, min(date_login) as date_login
    from base
    group by 1
  ) as b2 
      on b1.user_id = b2.user_id and b1.date_login = b2.date_login
  
),

overall as (
  select *
  from (
    select 
      paid_ffa_last_ad_click_dtime as touchtime,
      paid_ffa_type as product,
      email,
      userid,
      paid_ffa_channel as ad_platform,
      paid_ffa_campaign_name as campaign,
      paid_ffa_campaign_id as campaign_id,
      paid_ffa_su_dtime as event_first_timestamp,
      paid_ffa_campaign_type as source
    
    from marketing.fct_pfmkt_user_view_ffa 
    where 1=1
      and paid_ffa_type is not null

    union all

    select 
      new_core_last_ad_click_dtime as touchtime,
      'new core' as product,
      email,
      userid,
      new_core_channel as ad_platform,
      new_core_campaign_name as campaign,
      new_core_campaign_id as campaign_id,
      new_core_SU_dtime as event_first_timestamp,
      new_core_campaign_type as source
    
    from marketing.fct_pfmkt_user_view_ffa 
    where 1=1
      and new_core_channel is not null
  )
),

--To find out at the end, among the users who have cb membership, who has a taken/funded a loan
cb_loan_taken as (
  select
    a.userid,
    b.email,
    min(date(approveddate)) as first_date,
    min(approveddate) as first_timestamp
  from ml_finance.fpall_ll a 
  join lion1.user b on a.userid = _id
  where isfunded = 1 
    and b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
  group by 1,2
  having min(date(approveddate)) is not null
),

--To find out among those who have IC SU, and has taken 1st instacash
user_first_ic1 as(
  select 
    a.user_id as userid, 
    min(date(a.created_at)) as first_date,
    min(a.created_at) as first_timestamp
  from cashadvance_marketing.fct_first_ic_taken a
  join ml_public.addonhistory b on a.user_id = b.userid and date(a.created_at) >= date(b.createdon)
  where true
  group by 1
  having min(date(a.created_at)) is not null
),

user_first_ic as(
  select 
    a.userid, 
    b.email,
    a.first_date,
    a.first_timestamp
  from user_first_ic1 a
  join lion1.user b on a.userid = b._id
  where true
    and b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
),

-- Funded Roar Money Accounts

rm_earliest_deposit_dates as (
  select distinct
    a.ml_user_id as userid,
    b.email as email,
    min(date(a.transaction_created)) as first_date,
    min(a.transaction_created) as first_timestamp
  from dda2_raw.transaction a
  left join lion1.user b on a.ml_user_id = b._id
  where true
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
    and a.type + a.o_type in (
     select trans_type from (
        select distinct 
          *, 
          type + o_type as trans_type
        from (
          select 
            type, 
            o_type 
          from dda2_raw.transaction
          where true
            and type = 'pmt' --AND o_type != 'IP'

          union all 

          select 
            type, 
            o_type
          from dda2_raw.transaction
          where true
            and type = 'adj' 
            and o_type = 'NC'
        )
      )
    )
    and a.amount > 0 
  group by 1,2
),

total_cost as (
  select
    *
  from (
    --Facebook
    select
      date(date_stop) as date_stop,
      campaign_names.name,
      ads_data.campaign_id,
      'facebook' as ad_platform,
      sum(spend) as total_cost,
      sum(link_clicks) as total_clicks,
      sum(impressions) as total_impressions
    from (
      select distinct 
        ad_id, 
        date_stop, 
        spend, 
        link_clicks, 
        impressions
      from facebookads.insights
      where true
        and date_stop >= '2020-01-01'
    ) cost_data
    inner join (
      select distinct 
        id, 
        campaign_id
      from facebookads.ads
    ) ads_data on cost_data.ad_id = ads_data.id
    inner join (
      select distinct 
        id, 
        name, 
        split_part(name, '_|_', 8) as product
      from facebookads.campaigns
    ) campaign_names on ads_data.campaign_id = campaign_names.id
    group by 1,2,3,4

    union all

    --Google Ads
    select
      date(date_stop) as date_stop,
      campaign_names.name,  
      cost_data.campaign_id,
      'google' as ad_platform,    
      sum(cost) / 1000000.0 as total_cost,
      sum(clicks) as total_clicks,
      sum(impressions) as total_impressions
    from(
      select 
        campaign_id, 
        date_stop, 
        received_at, 
        cost, clicks, 
        impressions, 
        row_number() OVER (PARTITION by campaign_id, date_stop ORDER BY received_at DESC) as rn 
      from adwords.campaign_performance_reports
      where true
        and date_stop >= '2020-01-01'
    ) cost_data
    inner join(
      select distinct 
        id, 
        name, 
        split_part(name, '_|_', 8) as product
      from adwords.campaigns 
    ) campaign_names on cost_data.campaign_id = campaign_names.id
    where true
      and rn = 1
    group by 1,2,3,4

    union all

    --Bing Ads
    select 
      date(timeperiod) as date_stop,
      campaignname as name,
      cast(campaignid as varchar) as campaign_id,
      'bing' as ad_platform,
      sum(spend) as total_cost,
      sum(clicks) as total_clicks,
      sum(impressions) as total_impressions
    from (
      select distinct 
        timeperiod,
        campaignname,
        campaignid,
        spend,
        clicks,
        impressions
      from bingads.campaign_performance
      )
    where true
      and date(timeperiod) >= '2020-01-01'
    group by 1,2,3,4

    union all

    select 
      date("date") as date_stop,
      metadata_campaignname as name,
      cast(metadata_campaignid as varchar) as campaign_id,
      'apple' as ad_platform,
      sum(localspend_amount) as total_cost,
      sum(taps) as total_clicks,
      sum(impressions) as total_impressions
    from ds_growth.applesearchads_reports_campaign
    where true 
      and "date" >= '2020-01-01'
    group by 1,2,3,4

    union all

    select distinct 
      start_date as date_stop,
      c.name,
      s.id as campaign_id,
      'snapchat' as ad_platform,
      spend_usd as total_cost,
      swipes as total_clicks,
      impressions as total_impressions
    from snapchat.campaign_stat s
    left join snapchat.campaign c on c.id = s.id
    where true 
      and date(s.start_time) >= '2020-01-01'
    
    union all
    
    select
    
      date(a."date") as date_stop,
      a.campaign_c as name,
      b.campaign_id,
      case when a.media_source_pid = 'liftoff_int' then 'liftoff'
           when a.media_source_pid = 'reddit_int' then 'reddit'
      end as ad_platform,
      sum(a.total_cost) as total_cost,
      sum(cast(a.clicks as integer)) as total_clicks,
      sum(cast(a.impressions as integer)) as total_impressions

    from appsflyerapi.campaign as a
    left join (
      select distinct
        campaign,
        campaign_id,
        row_number() over (partition by campaign, campaign_id order by event_time desc) as rn
      from appsflyerapi.installs_report
      where date(event_time) >= current_date - interval '30 days'
    ) as b
      on a.campaign_c = b.campaign 
        and b.rn = 1
    where true
      and a.media_source_pid in ('liftoff_int','reddit_int')
      and date(a."date") >= '2020-01-01'
    group by 1,2,3,4
  )
)

select
  [c.touchtime:month] as "Week Start Date",
  c.ad_platform,
--   case when device_type.os_type is null then 'N/A' else device_type.os_type end as os_type,
  cast(cost.total_cost as decimal(16,2)) as "Spend",  
  cast(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)*1.0/avg(cost.total_clicks) as decimal(16,4))as "New SU CVR %",
  count(distinct case when [c.touchtime:month] >= '2020-03-13' and c.product = 'new core' then c.email end) as "New Core SU",
  count(distinct case when c.product = 'roar money' then c.email end) as "RM SU",
  count(distinct case when c.product = 'cbplus' then c.email end) as "CB+ SU",
  count(distinct case when c.product = 'instacash' then c.email end) as "IC SU",
  count(distinct case when c.product = 'investment standalone' then c.email end) as "Wealth SU",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end) as "Total New SU #",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end),0) as decimal(16,2)) as "Paid CAC $",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)*1.0/nullif(count(distinct case when [c.touchtime:month] >= '2020-03-13' and c.product = 'new core' then c.email end),0) as "Core to SU %",

--   count(distinct case when c.product = 'roar money' then fundedrm.email end) as "RM Funded",
--   cast(count(distinct case when c.product = 'roar money' then fundedrm.email end)*1.0/nullif(count(distinct case when c.product = 'roar money' then c.email end),0) as decimal(16,4)) as "RM Funded %",
--   count(distinct case when c.product = 'cbplus' then cbloan.email end) as "CB+ Funded",
--   cast(count(distinct case when c.product = 'cbplus' then cbloan.email end)*1.0/nullif(count(distinct case when c.product = 'cbplus' then c.email end),0) as decimal(16,4)) as "CB+ Funded %",
--   count(distinct case when c.product = 'instacash' then takeic.email end) as "Ic Taken",
--   cast(count(distinct case when c.product = 'instacash' then takeic.email end)*1.0/nullif(count(distinct case when c.product = 'instacash' then c.email end),0) as decimal(16,4)) as "IC Taken %",
  sum(case when user_view.total_net_rev is not null then user_view.total_net_rev end) as "ToDate NetRev",
  "ToDate NetRev"/nullif("Total New SU #",0) as "ToDate NetRev/Total SU",
  "ToDate NetRev/Total SU"/ (case when datediff('month', [c.touchtime:month], current_date) is null then 1 else datediff('month', [c.touchtime:month], current_date) + 1 end) as "Avg NetRev/Total SU"
  
from overall c 
-- left join ios_android_login as device_type
--   on device_type.email = c.email
left join (
  select 
    [date_stop:month] as week_start_date, 
    ad_platform,
    sum(total_cost) as total_cost,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from total_cost
  group by 1,2
  having sum(total_cost) > 0
) cost 
  on [c.touchtime:month] = cost.week_start_date
    and c.ad_platform = cost.ad_platform
left join cb_loan_taken cbloan 
  on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
left join user_first_ic takeic 
  on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
left join rm_earliest_deposit_dates fundedrm 
  on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime
left join marketing.fct_user_centric_view as user_view
  on user_view.email = c.email

where true 
--   and c.last_touch_for_prod = 1
  and [c.touchtime:month] between date('2020-01-01') and date('2020-11-30')
  and c.ad_platform not in ('bing', 'reddit', 'liftoff')
--   and device_type.os_type is not null
  
group by 1,2,3
order by 1 desc, 2,3 asc