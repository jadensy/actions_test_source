--no_cache
with cbp_campaigns as (
  select distinct campaign_id
  from brazeapi.campaign_details_tags
  where campaign_tag = 'cb plus'
  )

select
  a.campaign_name
  , sent
  , tapped
  , bounced

  , direct_cbonboarding dir_cbo
  , direct_cbsuccess dir_cbs
  , direct_cbonboarding * 1.0 / tapped as dir_cbo_rate
  , direct_cbsuccess * 1.0 / tapped as dir_cbs_rate

  , indirect_cbonboarding indir_cbo
  , indirect_cbsuccess indir_cbs
  , indirect_cbonboarding * 1.0 / sent as indir_cbo_rate
  , indirect_cbsuccess * 1.0 / sent as indir_cbs_rate

  , dir_cbo + indir_cbo as total_cbo
  , dir_cbs + indir_cbs as total_cbs

FROM (select campaign_name, count(distinct lower(user_id)) as sent from braze.push_notification_sent where campaign_id in (select * from cbp_campaigns) group by 1) AS a
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as tapped from braze.push_notification_tapped where campaign_id in (select * from cbp_campaigns) group by 1) AS b ON a.campaign_name = b.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as bounced from braze.push_notification_bounced where campaign_id in (select * from cbp_campaigns) group by 1) AS c ON a.campaign_name = c.campaign_name
LEFT JOIN ( -- Direct Conversion CBSuccess
  select distinct
    bpnt.campaign_id
  , bpnt.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) direct_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) direct_cbsuccess
  from braze.push_notification_tapped bpnt
  left join prod.cbonboarding_start cbo on bpnt.user_id = cbo.user_id and bpnt.timestamp <= cbo.timestamp and datediff(hour, bpnt.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and bpnt.timestamp <= cbs.timestamp and datediff(hour, bpnt.timestamp, cbs.timestamp) <= 24
  where bpnt.campaign_id in (select * from cbp_campaigns)
  group by 1,2
  ) AS h ON a.campaign_name = h.campaign_name
LEFT JOIN ( -- Indirect Conversion CBSuccess
  select distinct
    bpns.campaign_id
  , bpns.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) indirect_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) indirect_cbsuccess
  from braze.push_notification_sent bpns
  left join prod.cbonboarding_start cbo on bpns.user_id = cbo.user_id and bpns.timestamp <= cbo.timestamp and datediff(hour, bpns.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and bpns.timestamp <= cbs.timestamp and datediff(hour, bpns.timestamp, cbs.timestamp) <= 24
  where bpns.campaign_id in (select * from cbp_campaigns)
  group by 1,2
  ) AS i ON a.campaign_name = i.campaign_name
ORDER BY 1