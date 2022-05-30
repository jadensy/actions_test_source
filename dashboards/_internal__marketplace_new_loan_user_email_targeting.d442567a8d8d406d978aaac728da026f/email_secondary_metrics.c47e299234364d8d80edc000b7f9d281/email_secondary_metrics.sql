with mapping as (
  select _id as userid,
         email
  from lion1.user
  where brand='ml'
  and deleted <> 1
)

, max_ef_data AS (
  SELECT MAX(clicked_at) AS max_timestamp
  FROM marketplace.fct_conversion_revenue
)

, exp_period AS (
  SELECT 
    campaign_id,
    userid,
    min(event_timestamp) as min_exp_timestamp,
    min(event_timestamp) + 7 as max_exp_timestamp
  FROM marketplace.fct_intervention_engagement_events
  WHERE medium = 'email'
    AND event = 'deliver'
    AND campaign_id in ('3894065', '3905910', '3905916', '3862445') 
  GROUP BY 1, 2
)

, email_engagement AS (
  SELECT 
    engagement.userid, 
    event, 
    event_timestamp,
    engagement.campaign_id, 
    CASE 
      WHEN engagement.campaign_id = '3894065' THEN 'treatment_1_low_income'
      WHEN engagement.campaign_id = '3905910' THEN 'treatment_2_low_bv5_score'
      WHEN engagement.campaign_id = '3905916' THEN 'treatment_3_has_liability'
      WHEN engagement.campaign_id = '3862445' THEN 'control_leadgen_loan'
    ELSE null END AS assignment_group,
    min_exp_timestamp,
    max_exp_timestamp
  FROM
    (
    SELECT *, split_part(url, '/', 3) AS sub_url
    FROM marketplace.fct_intervention_engagement_events
    WHERE medium = 'email'
    AND event <> 'click'
    AND campaign_id in ('3894065', '3905910', '3905916', '3862445') 
    UNION ALL
    SELECT *, split_part(url, '/', 3) AS sub_url
    FROM marketplace.fct_intervention_engagement_events
    WHERE medium = 'email'
    AND event = 'click'
    AND len(sub_url) > 0
    AND sub_url NOT SIMILAR TO '%(backButton|action?type=LOAN|moneylion.zendesk.com|www.moneylion.com|zoom.us|dismiss|action?type=LOAN)%'
    AND campaign_id in ('3894065', '3905910', '3905916', '3862445') 
    ) as engagement -- question: to remove onelink and dashboard filters? moneylion.onelink.me|dashboard.moneylion.com|
  JOIN mapping ON mapping.userid = engagement.userid
  LEFT JOIN exp_period 
    ON engagement.campaign_id = exp_period.campaign_id 
    AND engagement.userid = exp_period.userid 
    AND event_timestamp >= exp_period.min_exp_timestamp 
    AND event_timestamp <= exp_period.max_exp_timestamp
  -- treatment groups ('3894065', '3905910', '3905916')
  -- control groups '3873402', '3862445')
)

, total_email_engagement AS (
  SELECT 
    assignment_group,
    count(distinct case when event = 'deliver' then userid else null end) as uniq_email_delivered_incl_bounced,
    count(distinct case when event = 'deliver' then userid else null end) - count(distinct case when event = 'bounce' then userid else null end) as uniq_email_delivered,
    count(distinct case when event = 'open' then userid else null end) as uniq_email_opened,
    count(distinct case when event = 'click' then userid else null end) as uniq_email_clicked,
    count(distinct case when event = 'bounce' then userid else null end) as uniq_email_bounced,
    count(distinct case when event = 'unsubscribe' then userid else null end) as uniq_email_unsubscribed,
    count(distinct case when event = 'spam' then userid else null end) as uniq_email_spammed
  FROM email_engagement
  GROUP BY 1
  ORDER BY 1   
)

, final_table AS (
  SELECT 
    a.assignment_group,
    uniq_email_delivered_incl_bounced,
    uniq_email_delivered,
    uniq_email_opened,
    uniq_email_clicked,
    ROUND((uniq_email_opened * 1.00 / NULLIF(uniq_email_delivered, 0)), 3) AS email_open_rate,
    ROUND((uniq_email_clicked * 1.00 / NULLIF(uniq_email_delivered, 0)), 3) AS email_ctr,
    ROUND((uniq_email_bounced * 1.00 / NULLIF(uniq_email_delivered_incl_bounced, 0)), 3) AS email_bounce_rate,
    ROUND((uniq_email_unsubscribed * 1.00 / NULLIF(uniq_email_delivered, 0)), 3) AS email_unsubscribe_rate,
    ROUND((uniq_email_spammed * 1.00 / NULLIF(uniq_email_delivered, 0)), 3) AS email_spam_rate
  FROM total_email_engagement a
  ORDER BY 1
)

select * from final_table