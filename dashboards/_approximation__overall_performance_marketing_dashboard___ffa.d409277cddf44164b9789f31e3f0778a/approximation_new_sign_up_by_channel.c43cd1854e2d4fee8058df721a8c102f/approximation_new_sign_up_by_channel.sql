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
    [install_date:week] as install_week, 
    os_version_group, 
    sum(num_users) as num_installs,
    sum(case when last_updated_att_status = 'authorized' then num_users end)* 1.0 / sum(case when last_updated_att_status in ('authorized','restricted','denied') then num_users end) as opt_in_rate,
    sum(case when last_updated_idfa = 'yes' then num_users end)* 1.0 / sum(num_users) as "IDFA Available? includes non determined"

  --   sum(case when last_updated_att_status in ('authorized', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_for_14_5,
  --   sum(case when last_updated_att_status in ('authorized', 'not_determined', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_below_14_5

  from att_prelim_compiled
  where os_version_group = 'ios 14.5 and above'
  group by 1,2
  order by 1 desc, 2 asc
  
),


total_cost as (
  select
    *,
    [date_stop:week] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
),

non_ios14_results as (

  select
    [c.touchtime:week] as week_start_date,
    c.ad_platform,
    cast(cost.total_cost as decimal(16,2)) as cost,
    avg(cost.total_impressions) as ad_impression,
    avg(cost.total_clicks) as ad_clicks,
    cast(avg(cost.total_clicks)*1.0/avg(cost.total_impressions) as decimal(16,4)) as ad_ctr,  
    cast(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)*1.0/avg(cost.total_clicks) as decimal(16,4))as new_su_conv,
    avg(cost.total_cost)*1000.0/nullif(avg(cost.total_impressions),0) as cpm,
    avg(cost.total_cost)*1.0/nullif(avg(cost.total_clicks),0) as cpc,
    count(distinct case when [c.touchtime:week] >= '2020-03-13' and c.product = 'new core' then c.email end) as new_core_su,
    count(distinct case when c.product = 'roar money' then c.email end) as rm_su,
    count(distinct case when c.product = 'cbplus' then c.email end) as cb_su,
    count(distinct case when c.product = 'instacash' then c.email end) as ic_su,
    count(distinct case when c.product = 'investment standalone' then c.email end) as wealth_su,
    count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end) as total_new_su,
    cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end),0) as decimal(16,2)) as paid_cac,
    count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)*1.0/nullif(count(distinct case when [c.touchtime:week] >= '2020-03-13' and c.product = 'new core' then c.email end),0) as core_to_su

  from overall c 
  left join (
    select 
      [date_stop:week] as week_start_date, 
      ad_platform,
      sum(total_cost) as total_cost,
      sum(total_clicks) as total_clicks,
      sum(total_impressions) as total_impressions
    from total_cost
    group by 1,2
    having sum(total_cost) > 0
  ) cost 
    on [c.touchtime:week] = cost.week_start_date
      and c.ad_platform = cost.ad_platform

  where true 
  --   and c.last_touch_for_prod = 1
    and [c.touchtime:week] >= date('2020-01-01')

  group by 1,2,3
),

ios14_results as (

  select
    [c.touchtime:week] as week_start_date,
    c.ad_platform,
    round(count(distinct case when [c.touchtime:week] >= '2020-03-13' and c.product = 'new core' then c.email end) * 1.0 / avg(att_results.opt_in_rate),0) as new_core_su,
    round(count(distinct case when c.product = 'roar money' then c.email end)* 1.0 / avg(att_results.opt_in_rate),0) as rm_su,
    round(count(distinct case when c.product = 'cbplus' then c.email end)* 1.0 / avg(att_results.opt_in_rate),0) as cb_su,
    round(count(distinct case when c.product = 'instacash' then c.email end)* 1.0 / avg(att_results.opt_in_rate),0) as ic_su,
    round(count(distinct case when c.product = 'investment standalone' then c.email end)* 1.0 / avg(att_results.opt_in_rate),0) as wealth_su,
    round(count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end)* 1.0 / avg(att_results.opt_in_rate),0) as total_new_su


  from ios14_overall c 

  left join att_results 
    on att_results.install_week = [c.touchtime:week]
  
  where true 
    and [c.touchtime:week] >= date('2020-01-01')

  group by 1,2
),

results as (

  select 



    a.week_start_date,
    a.ad_platform,
    a.cost as total_cost,
    a.rm_su + coalesce(b.rm_su,0) as roar_money_count,
    a.cb_su + coalesce(b.cb_su,0) as cbplus_count,
    a.ic_su + coalesce(b.ic_su,0) as ic_count,
    a.wealth_su + coalesce(b.wealth_su,0) as wealth_count,
    a.total_new_su + coalesce(b.total_new_su,0) as total_new_su



  from non_ios14_results as a
  left join ios14_results as b
    on a.week_start_date = b.week_start_date
      and a.ad_platform = b.ad_platform

)



select
  c.week_start_date,
  c.ad_platform,
  c.total_new_su as total_membership_sign_up
  
from results c 
-- left join cb_loan_taken cbloan on cbloan.email = c.email
-- left join user_first_ic takeic on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
where 1=1 
  and week_start_date >= date('2020-01-01')
order by 1,2 asc