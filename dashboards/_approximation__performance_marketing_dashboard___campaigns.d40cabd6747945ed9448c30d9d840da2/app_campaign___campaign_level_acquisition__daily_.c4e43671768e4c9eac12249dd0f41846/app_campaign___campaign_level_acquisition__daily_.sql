-- This dashboard/chart has been updated by DE to apply the DDA2 schema migration changes on 2020-11-01 03:18:20.170922 
-- For more info, see https://moneylion.atlassian.net/wiki/spaces/DE/pages/1809711844/DDA2+Schema+Migration+NOV+2020

with overall as (
  select a.*
  from (select * from [pf_mkt_attr_users_jump450]) as a
  left join (select * from [pf_mkt_ios14_campaigns]) as b
    on b.ad_platform = a.ad_platform 
      and a.campaign_id = b.campaign_id
  where true
    and b.campaign_id is null
),

ios14_overall as (
  select a.*
  from (select * from [pf_mkt_attr_users_jump450]) as a
  join (select * from [pf_mkt_ios14_campaigns]) as b
    on b.ad_platform = a.ad_platform 
      and a.campaign_id = b.campaign_id
),

install_results as (
  select distinct
    install_time,
    os_version,
    cast(nullif(split_part(os_version, '.', 1), '') as numeric) * 1.0 + coalesce(cast(nullif(split_part(os_version, '.', 2), '') as numeric),0) * 1.0 / 10 + coalesce(cast(nullif(split_part(os_version, '.', 3), '') as numeric),0) * 1.0 / 100 as trunc_os_version,
    att,
    is_lat,
    case when idfa is null then 'no' else 'yes' end as "idfa available",
    appsflyer_id

  from appsflyerapi.organic_installs_report
  where true
    and install_time >= '2021-04-26'
    and platform = 'ios'


  union all

  select distinct
    install_time,
    os_version,
    cast(nullif(split_part(os_version, '.', 1), '') as numeric) * 1.0 + coalesce(cast(nullif(split_part(os_version, '.', 2), '') as numeric),0) * 1.0 / 10 + coalesce(cast(nullif(split_part(os_version, '.', 3), '') as numeric),0) * 1.0 / 100 as trunc_os_version,
    att,
    is_lat,
    case when idfa is null then 'no' else 'yes' end as "idfa available",
    appsflyer_id

  from appsflyerapi.installs_report
  where true
    and install_time >= '2021-04-26'
    and platform = 'ios'
  
),

in_app_events_result as (
  
  select
    os_version,
    cast(nullif(split_part(os_version, '.', 1), '') as numeric) * 1.0 + coalesce(cast(nullif(split_part(os_version, '.', 2), '') as numeric),0) * 1.0 / 10 + coalesce(cast(nullif(split_part(os_version, '.', 3), '') as numeric),0) * 1.0 / 100 as trunc_os_version,
    att,
    case when idfa is null then 'no' else 'yes' end as "idfa available",
    appsflyer_id,
    row_number() over (partition by appsflyer_id order by event_time desc) as rn

  from appsflyerapi.organic_in_app_events_report
  where true
    and install_time >= '2021-04-26'
    and platform = 'ios'
  
  union all
  
  select distinct
    os_version,
    cast(nullif(split_part(os_version, '.', 1), '') as numeric) * 1.0 + coalesce(cast(nullif(split_part(os_version, '.', 2), '') as numeric),0) * 1.0 / 10 + coalesce(cast(nullif(split_part(os_version, '.', 3), '') as numeric),0) * 1.0 / 100 as trunc_os_version,
    att,
    case when idfa is null then 'no' else 'yes' end as "idfa available",
    appsflyer_id,
    row_number() over (partition by appsflyer_id order by event_time desc) as rn

  from appsflyerapi.in_app_events_report
  where true
    and install_time >= '2021-04-26'
    and platform = 'ios'
  
),

att_prelim_compiled as (
  
  select 
  --   a.os_version,
    [a.install_time:date] as install_date,
  --   a.trunc_os_version as os_version,
    case when a.trunc_os_version < 14 then 'below ios 14'
    when a.trunc_os_version >= 14 and a.trunc_os_version < 14.5 then 'ios 14 to 14.4'
    when a.trunc_os_version >= 14.5 then 'ios 14.5 and above'
    else cast(a.trunc_os_version as varchar) end as os_version_group,



    a.att as install_att_status,
    a.is_lat install_lat_status,
    a."idfa available" as install_idfa_avail,

    b.att as latest_event_att_status,
    b."idfa available" as latest_event_idfa_avail,

    coalesce(latest_event_att_status, install_att_status) as last_updated_att_status,
    coalesce(b."idfa available", a."idfa available") as last_updated_idfa,

    count(distinct a.appsflyer_id) as num_users
  from install_results as a
  left join in_app_events_result as b
    on a.appsflyer_id = b.appsflyer_id
      and b.rn = 1
  -- where 
  --   cast(trunc_os_version as float) >= 14.5
  group by 1,2,3,4,5,6,7,8
-- order by 1,2 desc
),

att_results as (
  
  select 
    [install_date:date] as install_date, 
    os_version_group, 
    sum(num_users) as num_installs,
    sum(case when last_updated_att_status = 'authorized' then num_users end)* 1.0 / nullif(sum(case when last_updated_att_status in ('authorized','restricted','denied') then num_users end),0) as opt_in_rate,
    sum(case when last_updated_idfa = 'yes' then num_users end)* 1.0 / nullif(sum(num_users),0) as "IDFA Available? includes non determined"

  --   sum(case when last_updated_att_status in ('authorized', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_for_14_5,
  --   sum(case when last_updated_att_status in ('authorized', 'not_determined', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_below_14_5

  from att_prelim_compiled
  where os_version_group = 'ios 14.5 and above'
  group by 1,2
  order by 1 desc, 2 asc
  
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
    *,
    [date_stop:week] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
),

[pf_mkt_app_install_w_tapjoy],

non_ios14_results as (

  select distinct
    coalesce(c.week_start_date, cost.week_start_date) as dt,
    coalesce(c.ad_platform, cost.ad_platform) as ad_platform,
    coalesce(c.campaign, cost.campaign) as campaign,
    coalesce(c.campaign_id, cost.campaign_id) as campaign_id,


    cast(cost.total_cost as decimal(16,2)) as "Cost",
    cost.total_impressions as "Ad Impressions",
    cost.total_clicks as "Ad Clicks",
    install.app_install as "App Install",
    cast(cost.total_clicks*1.0/cost.total_impressions as decimal(16,4)) as "Ad CTR %",  
    cast(c."Total New SU" * 1.0/cost.total_clicks as decimal(16,4))as "New SU CVR %",

    c."New Core SU",
    c."RM SU",
    c."CB+ SU",
    c."IC SU",
    c."Wealth SU",
    c."Total New SU",
    cast(cost.total_cost*1.0/nullif(c."Total New SU",0) as decimal(16,2)) as "Paid CAC",
    c."RM Funded",
    c."RM Funded %",
    c."CB+ Funded",
    c."CB+ Funded %",
    c."IC Taken",
    c."IC Taken %"



  from (
    select 

      [c.touchtime:date] as week_start_date,
      c.ad_platform,
      c.campaign,
      c.campaign_id,
      count(distinct case when [c.touchtime:week] >= '2020-03-13' and c.product = 'new core' then c.email end) as "New Core SU",
      count(distinct case when c.product = 'roar money' then c.email end) as "RM SU",
      count(distinct case when c.product = 'cbplus' then c.email end) as "CB+ SU",
      count(distinct case when c.product = 'instacash' then c.email end) as "IC SU",
      count(distinct case when c.product = 'investment standalone' then c.email end) as "Wealth SU",
      count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end) as "Total New SU",

      count(distinct case when c.product = 'roar money' then fundedrm.email end) as "RM Funded",
      cast(count(distinct case when c.product = 'roar money' then fundedrm.email end)*1.0/nullif(count(distinct case when c.product = 'roar money' then c.email end),0) as decimal(16,4)) as "RM Funded %",
      count(distinct case when c.product = 'cbplus' then cbloan.email end) as "CB+ Funded",
      cast(count(distinct case when c.product = 'cbplus' then cbloan.email end)*1.0/nullif(count(distinct case when c.product = 'cbplus' then c.email end),0) as decimal(16,4)) as "CB+ Funded %",
      count(distinct case when c.product = 'instacash' then takeic.email end) as "IC Taken",
      cast(count(distinct case when c.product = 'instacash' then takeic.email end)*1.0/nullif(count(distinct case when c.product = 'instacash' then c.email end),0) as decimal(16,4)) as "IC Taken %"

    from overall c
    left join cb_loan_taken cbloan 
      on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
    left join user_first_ic takeic 
      on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
    left join rm_earliest_deposit_dates fundedrm 
      on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime
    where 1=1
      and source = 'app campaign'
    group by 1,2,3,4
  )c 

  full outer join (
    select 
      [date_stop:date] as week_start_date,
      name as campaign,
      campaign_id,
      ad_platform,
      sum(total_cost) as total_cost,
      sum(total_clicks) as total_clicks,
      sum(total_impressions) as total_impressions
    from total_cost
    where true
      and (lower(campaign) not like '%webflow%' and lower(campaign) not like '%homepage%')
    group by 1,2,3,4
    having sum(total_cost) > 0
  ) cost on c.week_start_date = cost.week_start_date and c.ad_platform = cost.ad_platform and c.campaign_id = cost.campaign_id


  left join (
    select distinct  
      [start_timestamp:date] as week_start_date,
      ad_platform,
  --     campaign,
      campaign_id,
      count(distinct installs) as app_install
    from (
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events 
      union all
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events_retargeting
      )a
    group by 1,2,3
    )install 
      on install.week_start_date = coalesce(c.week_start_date, cost.week_start_date)  
        and install.ad_platform = coalesce(c.ad_platform, cost.ad_platform) 
  --       and install.campaign = coalesce(c.campaign, cost.campaign) 
        and install.campaign_id = coalesce(c.campaign_id, cost.campaign_id)
  --       and install.campaign_id = coalesce(c.campaign_id, cost.campaign_id)
  where 1=1 

),

ios14_results as (
  
  select distinct
    coalesce(c.week_start_date, cost.week_start_date) as dt,
    coalesce(c.ad_platform, cost.ad_platform) as ad_platform,
    coalesce(c.campaign, cost.campaign) as campaign,
    coalesce(c.campaign_id, cost.campaign_id) as campaign_id,


    cast(cost.total_cost as decimal(16,2)) as "Cost",
    cost.total_impressions as "Ad Impressions",
    cost.total_clicks as "Ad Clicks",
    install.app_install as "App Install",
    cast(cost.total_clicks*1.0/nullif(cost.total_impressions,0) as decimal(16,4)) as "Ad CTR %",  
    cast(c."Total New SU" * 1.0/nullif(cost.total_clicks,0) as decimal(16,4))as "New SU CVR %",

    c."New Core SU",
    c."RM SU",
    c."CB+ SU",
    c."IC SU",
    c."Wealth SU",
    c."Total New SU",
    cast(cost.total_cost*1.0/nullif(c."Total New SU",0) as decimal(16,2)) as "Paid CAC",
    c."RM Funded",
    c."RM Funded %",
    c."CB+ Funded",
    c."CB+ Funded %",
    c."IC Taken",
    c."IC Taken %"



  from (
    select 

      [c.touchtime:date] as week_start_date,
      c.ad_platform,
      c.campaign,
      c.campaign_id,
      round(count(distinct case when [c.touchtime:week] >= '2020-03-13' and c.product = 'new core' then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "New Core SU",
      round(count(distinct case when c.product = 'roar money' then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "RM SU",
      round(count(distinct case when c.product = 'cbplus' then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "CB+ SU",
      round(count(distinct case when c.product = 'instacash' then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "IC SU",
      round(count(distinct case when c.product = 'investment standalone' then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "Wealth SU",
      round(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "Total New SU",

      round(count(distinct case when c.product = 'roar money' then fundedrm.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "RM Funded",
      "RM Funded"*1.0/nullif("RM SU",0) as "RM Funded %",
      round(count(distinct case when c.product = 'cbplus' then cbloan.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "CB+ Funded",
      "CB+ Funded"*1.0/nullif("CB+ SU",0) as "CB+ Funded %",
      round(count(distinct case when c.product = 'instacash' then takeic.email end)* 1.0 / nullif(avg(att_results.opt_in_rate),0),0) as "IC Taken",
      "IC Taken"*1.0/nullif("IC SU",0) as "IC Taken %"

    from ios14_overall c
    left join att_results 
      on att_results.install_date = [c.touchtime:date]
    left join cb_loan_taken cbloan 
      on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
    left join user_first_ic takeic 
      on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
    left join rm_earliest_deposit_dates fundedrm 
      on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime
    where 1=1
      and source = 'app campaign'
    group by 1,2,3,4
  )c 

  full outer join (
    select 
      [date_stop:date] as week_start_date,
      name as campaign,
      campaign_id,
      ad_platform,
      sum(total_cost) as total_cost,
      sum(total_clicks) as total_clicks,
      sum(total_impressions) as total_impressions
    from total_cost
    where true
      and (lower(campaign) not like '%webflow%' and lower(campaign) not like '%homepage%')
    group by 1,2,3,4
    having sum(total_cost) > 0
  ) cost on c.week_start_date = cost.week_start_date and c.ad_platform = cost.ad_platform and c.campaign_id = cost.campaign_id


  left join (
    select distinct  
      [start_timestamp:date] as week_start_date,
      ad_platform,
  --     campaign,
      campaign_id,
      count(distinct installs) as app_install
    from (
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events 
      union all
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events_retargeting
      )a
    group by 1,2,3
    )install 
      on install.week_start_date = coalesce(c.week_start_date, cost.week_start_date)  
        and install.ad_platform = coalesce(c.ad_platform, cost.ad_platform) 
  --       and install.campaign = coalesce(c.campaign, cost.campaign) 
        and install.campaign_id = coalesce(c.campaign_id, cost.campaign_id)
  --       and install.campaign_id = coalesce(c.campaign_id, cost.campaign_id)
  where 1=1 
  
)

select * from non_ios14_results
union all
select * from ios14_results


  order by 1 desc, 2,3,4,5 asc