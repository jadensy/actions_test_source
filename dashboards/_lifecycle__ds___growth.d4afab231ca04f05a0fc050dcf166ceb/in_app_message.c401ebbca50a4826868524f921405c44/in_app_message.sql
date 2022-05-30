--no_cache
with cbp_campaigns as (
  select distinct campaign_id
  from brazeapi.campaign_details_tags
  where campaign_tag = 'cb plus'
  )

select
  a.campaign_name
  , viewed impression
  , clicked_button0 button1_clicks
  , clicked_button1 button2_clicks

  , direct_cbonboarding dir_cbo
  , direct_cbsuccess dir_cbs
  , direct_cbonboarding * 1.0 / button2_clicks as dir_cbo_rate
  , direct_cbsuccess * 1.0 / button2_clicks as dir_cbs_rate

  , indirect_cbonboarding indir_cbo
  , indirect_cbsuccess indir_cbs
  , indirect_cbonboarding * 1.0 / impression as indir_cbo_rate
  , indirect_cbsuccess * 1.0 / impression as indir_cbs_rate

  , dir_cbo + indir_cbo as total_cbo
  , dir_cbs + indir_cbs as total_cbs

FROM (select campaign_name, count(distinct lower(user_id)) as viewed from braze.in_app_message_viewed where campaign_id in (select * from cbp_campaigns) group by 1) AS a
LEFT JOIN (select campaign_name, count(distinct case when button_id = 0 then lower(user_id) end) as clicked_button0 from braze.in_app_message_clicked where campaign_id in (select * from cbp_campaigns) group by 1) AS b ON a.campaign_name = b.campaign_name
LEFT JOIN (select campaign_name, count(distinct case when button_id = 1 then lower(user_id) end) as clicked_button1 from braze.in_app_message_clicked where campaign_id in (select * from cbp_campaigns) group by 1) AS c ON a.campaign_name = c.campaign_name
LEFT JOIN ( -- Direct Conversion CBSuccess
  select distinct
    biamc.campaign_id
  , biamc.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) direct_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) direct_cbsuccess
  from braze.in_app_message_clicked biamc
  left join prod.cbonboarding_start cbo on biamc.user_id = cbo.user_id and biamc.timestamp <= cbo.timestamp and datediff(hour, biamc.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and biamc.timestamp <= cbs.timestamp and datediff(hour, biamc.timestamp, cbs.timestamp) <= 24
  where biamc.campaign_id in (select * from cbp_campaigns)
  and button_id = 1
  group by 1,2
  ) AS h ON a.campaign_name = h.campaign_name
LEFT JOIN ( -- Indirect Conversion CBSuccess
  select distinct
    biamv.campaign_id
  , biamv.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) indirect_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) indirect_cbsuccess
  from braze.in_app_message_viewed biamv
  left join prod.cbonboarding_start cbo on biamv.user_id = cbo.user_id and biamv.timestamp <= cbo.timestamp and datediff(hour, biamv.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and biamv.timestamp <= cbs.timestamp and datediff(hour, biamv.timestamp, cbs.timestamp) <= 24
  where biamv.campaign_id in (select * from cbp_campaigns)
  group by 1,2
  ) AS i ON a.campaign_name = i.campaign_name
ORDER BY 1