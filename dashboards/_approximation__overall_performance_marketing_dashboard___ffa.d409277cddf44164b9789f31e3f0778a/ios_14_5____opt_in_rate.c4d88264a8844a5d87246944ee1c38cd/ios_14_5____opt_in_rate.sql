with install_results as (
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

result as (
  select 
  --   a.os_version,
    [a.install_time:week] as install_week,
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
)

select 
  install_week, 
  os_version_group, 
  sum(num_users) as num_installs,
  sum(case when last_updated_att_status = 'authorized' then num_users end)* 1.0 / sum(case when last_updated_att_status in ('authorized','restricted','denied') then num_users end) as opt_in_rate

--   sum(case when last_updated_att_status in ('authorized', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_for_14_5,
--   sum(case when last_updated_att_status in ('authorized', 'not_determined', 'af_authorized') then num_users end)* 1.0 / sum(num_users) as able_rate_below_14_5

from result
where os_version_group = 'ios 14.5 and above'
group by 1,2
order by 1 desc, 2 asc