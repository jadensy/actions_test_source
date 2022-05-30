--no_cache
with cbp_campaigns as (
  select distinct campaign_id
  from brazeapi.campaign_details_tags
  where campaign_tag = 'cb plus'
  )

select
  a.campaign_name
  , sent
  , delivered
  , opened
  , clicked
  , opened * 1.0 / delivered as open_rate
  , clicked * 1.0 / opened as click_rate

  , direct_cbonboarding dir_cbo
  , direct_cbsuccess dir_cbs
  , direct_cbonboarding * 1.0 / clicked as dir_cbo_rate
  , direct_cbsuccess * 1.0 / clicked as dir_cbs_rate

  , indirect_cbonboarding indir_cbo
  , indirect_cbsuccess indir_cbs
  , indirect_cbonboarding * 1.0 / opened as indir_cbo_rate
  , indirect_cbsuccess * 1.0 / opened as indir_cbs_rate

  , dir_cbo + indir_cbo as total_cbo
  , dir_cbs + indir_cbs as total_cbs

  , bounced
  , spam
  , unsub
  , unsub * 1.0 / opened as unsub_rate

FROM (select campaign_name, count(distinct lower(user_id)) as sent from braze.email_sent where campaign_id in (select * from cbp_campaigns) group by 1) AS a
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as delivered from braze.email_delivered where campaign_id in (select * from cbp_campaigns) group by 1) AS b ON a.campaign_name = b.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as opened from braze.email_opened where campaign_id in (select * from cbp_campaigns) group by 1) AS c ON a.campaign_name = c.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as clicked from braze.email_link_clicked where campaign_id in (select * from cbp_campaigns) group by 1) AS d ON a.campaign_name = d.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as bounced from braze.email_bounced where campaign_id in (select * from cbp_campaigns) group by 1) AS e ON a.campaign_name = e.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as spam from braze.email_marked_as_spam where campaign_id in (select * from cbp_campaigns) group by 1) AS f ON a.campaign_name = f.campaign_name
LEFT JOIN (select campaign_name, count(distinct lower(user_id)) as unsub from braze.unsubscribed where campaign_id in (select * from cbp_campaigns) group by 1) AS g ON a.campaign_name = g.campaign_name
LEFT JOIN ( -- Direct Conversion CBSuccess
  select distinct
    belc.campaign_id
  , belc.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) direct_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) direct_cbsuccess
  from braze.email_link_clicked belc
  left join prod.cbonboarding_start cbo on belc.user_id = cbo.user_id and belc.timestamp <= cbo.timestamp and datediff(hour, belc.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and belc.timestamp <= cbs.timestamp and datediff(hour, belc.timestamp, cbs.timestamp) <= 24
  where belc.campaign_id in (select * from cbp_campaigns)
  group by 1,2
  ) AS h ON a.campaign_name = h.campaign_name
LEFT JOIN ( -- Indirect Conversion CBSuccess
  select distinct
    beo.campaign_id
  , beo.campaign_name
  , count(distinct case when cbo.timestamp is not null then cbo.user_id end) indirect_cbonboarding
  , count(distinct case when cbs.timestamp is not null then cbs.user_id end) indirect_cbsuccess
  from braze.email_opened beo
  left join prod.cbonboarding_start cbo on beo.user_id = cbo.user_id and beo.timestamp <= cbo.timestamp and datediff(hour, beo.timestamp, cbo.timestamp) <= 24
  left join prod.cbsuccess cbs on cbo.user_id = cbs.user_id and beo.timestamp <= cbs.timestamp and datediff(hour, beo.timestamp, cbs.timestamp) <= 24
  where beo.campaign_id in (select * from cbp_campaigns)
  group by 1,2
  ) AS i ON a.campaign_name = i.campaign_name
ORDER BY 1