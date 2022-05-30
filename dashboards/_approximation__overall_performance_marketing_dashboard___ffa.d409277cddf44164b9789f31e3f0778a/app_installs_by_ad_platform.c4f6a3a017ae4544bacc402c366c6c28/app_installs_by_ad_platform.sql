--no_cache
with [pf_mkt_app_install_w_tapjoy]

    select distinct  
      [date(start_timestamp):month] as month,
    ad_platform,
    count(distinct installs) as app_install
    from (
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events 
      union 
      select start_timestamp, campaign, campaign_id, installs, ad_platform
      from events_retargeting
      )a
    where campaign not like '%webflow%'
    and month between '2020-10-01' and '2021-05-01'
    group by 1,2
 order by 1 desc