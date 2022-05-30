with mapping as (
  select _id as userid,
         email
  from lion1.user
  where brand='ml'
  and deleted <> 1
)

, core_users AS (
  SELECT userid, email, first_timestamp, 'core' AS user_type
  FROM marketing.stg_user__new_core
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
    count(distinct case when event = 'deliver' then userid else null end) - count(distinct case when event = 'bounce' then userid else null end) as uniq_email_delivered,
    count(distinct case when event = 'open' then userid else null end) as uniq_email_opened,
    count(distinct case when event = 'click' then userid else null end) as uniq_email_clicked
    --count(distinct case when event = 'bounce' then userid else null end) as unq_email_bounced,
    --count(distinct case when event = 'unsubscribe' then userid else null end) as unq_email_unsubscribed,
    --count(distinct case when event = 'spam' then userid else null end) as unq_email_spammed
  FROM email_engagement
  GROUP BY 1
  ORDER BY 1   
)

, exp_user AS (
  SELECT 
    userid,
    assignment_group, 
    min_exp_timestamp,
    max_exp_timestamp,
    event,
    (CASE WHEN event = 'deliver' THEN min(event_timestamp) ELSE null END) as email_deliver_time,
    (CASE WHEN event = 'open' THEN min(event_timestamp) ELSE null END) as first_email_open_time,
    (CASE WHEN event = 'click' THEN min(event_timestamp) ELSE null END) as first_email_click_time
  FROM email_engagement
  GROUP BY 1, 2, 3, 4, 5
)

, engagement as (
  select assignment_group, 
         engagement.userid,
         engagement.event_timestamp,
         engagement.partner,
         engagement.event
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user 
  on engagement.userid = exp_user.userid 
    and event_timestamp >= exp_user.min_exp_timestamp 
  and event_timestamp <= exp_user.max_exp_timestamp
  --where medium <> 'dashboard_web'
)

, total_engagement as (
  select assignment_group, 
         count(case when event = 'view' then userid else null end) as total_view,
         count(case when event = 'click' then userid else null end) as total_click,
         count(distinct case when event = 'view' then userid else null end) as total_uniq_view,
         count(distinct case when event = 'click' then userid else null end) as total_uniq_click
  from engagement
  GROUP BY 1
)

, mktp_engagement as (
  select assignment_group, 
         count(case when event = 'view' then userid else null end) as mktp_view,
         count(case when event = 'click' then userid else null end) as mktp_click,
         count(distinct case when event = 'view' then userid else null end) as mktp_uniq_view,
         count(distinct case when event = 'click' then userid else null end) as mktp_uniq_click
  from engagement
  where partner not ilike 'LoanInATapCard' or partner not ilike 'Personal Loan of%'
  GROUP BY 1
)

, even_engagement as (
  select assignment_group, 
         count(case when event = 'view' then userid else null end) as even_view,
         count(case when event = 'click' then userid else null end) as even_click,
         count(distinct case when event = 'view' then userid else null end) as even_uniq_view,
         count(distinct case when event = 'click' then userid else null end) as even_uniq_click
  from engagement
  where partner ilike 'LoanInATapCard' or partner ilike 'Personal Loan of%'
  GROUP BY 1
)

, partner_rev as (
  SELECT partner_service_name, l30d_avg_revenue_per_conversion, conversion_date
  FROM marketplace_static.partner_avg_revenue_per_conversion
)

, other_mktp_conversion AS (
  select 
    assignment_group,
    count(fct.userid) as other_mktp_conv,
    count(distinct fct.userid) as other_mktp_uniq_conv,
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as other_mktp_rev
  from marketplace.fct_conversion_revenue as fct
  join exp_user 
    on fct.userid = exp_user.userid 
    and fct.clicked_at >= exp_user.min_exp_timestamp 
    and fct.clicked_at <= exp_user.max_exp_timestamp
  left join partner_rev
    on fct.partner_service_name = partner_rev.partner_service_name 
    and fct.converted_at::date = partner_rev.conversion_date
  where medium in ('marketplace', 'email', 'elise')
  group by 1
)

, even_avg_rev AS (
  SELECT SUM(payout_dollar) / COUNT(*) AS even_avg_rev
  FROM marketplace.fct_even_conversion_revenue
)

, even_conversion AS (
  SELECT 
    assignment_group,
    COUNT(even.userid) AS even_conv,
    COUNT(distinct even.userid) AS even_uniq_conv,
    COUNT(distinct case when type = 'loan' then even.userid else null end) AS even_loan_uniq_conv,
--     COUNT(distinct case when type = 'specialOffer' then even.userid else null end) AS even_specialoff_uniq_conv,
    round(sum(coalesce(even_avg_rev,0))::float,2) as even_rev
  FROM (
    select userid,
           booked_at,
           type,
           COALESCE(even_avg_rev, 0) AS even_avg_rev
    FROM marketplace.fct_even_conversion_revenue, even_avg_rev
  ) even
  JOIN exp_user 
    ON even.userid = exp_user.userid
    AND even.booked_at >= exp_user.min_exp_timestamp 
    AND even.booked_at <= exp_user.max_exp_timestamp
  GROUP BY 1
)

, enter_even_flow as (
  SELECT assignment_group, COUNT(distinct a.user_id) AS enter_even_flow
  FROM (
  SELECT user_id, "timestamp" AS event_timestamp
  FROM ios.loan_mpl_customize_loan_view

  UNION ALL

  SELECT user_id, "timestamp" AS event_timestamp  
  FROM android.loan_mpl_customize_loan_view
  ) a
  JOIN mapping b ON a.user_id = b.email
  JOIN exp_user ON b.userid  = exp_user.userid
    AND event_timestamp >= exp_user.min_exp_timestamp 
    AND event_timestamp <= exp_user.max_exp_timestamp
  LEFT JOIN core_users c ON a.user_id = c.email AND event_timestamp >= first_timestamp 
  GROUP BY 1
)

, submit_even_api as (
  SELECT assignment_group, COUNT(distinct a.user_id) AS users_submit_api
  FROM (
  SELECT user_id, "timestamp" AS event_timestamp
  FROM ios.loan_mpl_last_thing_see_offers

  UNION ALL

  SELECT user_id, "timestamp" AS event_timestamp  
  FROM android.loan_mpl_last_thing_see_offers
  ) a
  JOIN mapping b ON a.user_id = b.email
  JOIN exp_user ON b.userid  = exp_user.userid 
    AND a.event_timestamp >= exp_user.min_exp_timestamp 
    AND a.event_timestamp <= exp_user.max_exp_timestamp
  LEFT JOIN core_users c ON a.user_id = c.email AND event_timestamp >= first_timestamp 
  GROUP BY 1
)

, even_loan_offers AS (
  SELECT userid, 
         created_at
  FROM marketplace.fct_even_lender_offers
  GROUP BY 1,2
)

, user_offered_loans AS (
  SELECT assignment_group, COUNT(DISTINCT a.userid) AS users_receive_offer
  FROM (
    SELECT userid,
           created_at
    FROM even_loan_offers
  ) a
  JOIN mapping b ON a.userid = b.userid
  JOIN exp_user ON a.userid  = exp_user.userid 
    AND a.created_at >= exp_user.min_exp_timestamp 
    AND a.created_at <= exp_user.max_exp_timestamp
  GROUP BY 1
)

-- , even_loan_offers AS (
--   SELECT user_id, 
--          timestamp 'epoch' + CAST(created_at AS BIGINT)/1000 * interval '1 second' AS created_at_utc  
--   FROM marketplace_raw.offers
--   GROUP BY 1,2
-- )

-- , user_offered_loans AS (
--   SELECT assignment_group, COUNT(DISTINCT a.user_id) AS users_receive_offer
--   FROM (
--   SELECT user_id,
--          created_at_utc
--   FROM even_loan_offers) a
--   JOIN mapping b ON a.user_id = b.userid
--   JOIN exp_user ON a.user_id  = exp_user.userid 
--     AND a.created_at_utc >= exp_user.min_exp_timestamp 
--     AND a.created_at_utc <= exp_user.max_exp_timestamp
--   LEFT JOIN core_users c ON a.user_id = c.userid AND created_at_utc >= first_timestamp
--   GROUP BY 1
-- )

, user_clicked_events AS (
  SELECT 
    userid, 
--     (case when location = 'special-offers' then 'specialOffer' else 'loan' end) as type,
    event_timestamp
  FROM marketplace.fct_even_lender_engagement_events
  WHERE event = 'click'
  AND location <> 'special-offers'
)

, user_clicked_mpl_offers AS (
  SELECT 
    assignment_group, COUNT(DISTINCT a.userid) AS users_click_loan_offer
--     COUNT(DISTINCT case when type = 'loan' then a.userid else null end) AS users_click_loan_offer
--     COUNT(DISTINCT case when type = 'specialOffer' then a.userid else null end) AS users_click_special_offer
  FROM (
  SELECT userid, 
--          type,
         event_timestamp
  FROM user_clicked_events) a
  JOIN exp_user ON a.userid  = exp_user.userid 
    AND a.event_timestamp >= exp_user.min_exp_timestamp 
    AND a.event_timestamp <= exp_user.max_exp_timestamp
  LEFT JOIN core_users c ON a.userid = c.userid AND event_timestamp >= first_timestamp
  GROUP BY 1
)

-- , loan_applied AS (
--   SELECT lead_id, 
--          userid,
--          event_created_at
--   FROM marketplace.fct_even_lead_events
--   WHERE event_type = 'applied'
-- )

-- , user_applied_mpl_offers AS (
--   SELECT assignment_group, COUNT(DISTINCT a.userid) AS users_apply_offer
--   FROM (
--   SELECT userid, 
--          event_created_at
--   FROM loan_applied) a
--   JOIN exp_user ON a.userid  = exp_user.userid 
--     AND a.event_created_at >= exp_user.min_exp_timestamp 
--     AND a.event_created_at <= exp_user.max_exp_timestamp
--   GROUP BY 1
-- )

-- , loan_funded AS (
--   SELECT lead_id, 
--          userid,
--          event_created_at,
--          amount_in_dollars
--   FROM marketplace.fct_even_lead_events
--   WHERE event_type = 'funded'
-- )

-- , user_funded_mpl_offers AS (
--   SELECT assignment_group, COUNT(DISTINCT a.userid) AS users_funded_offer
--   FROM (
--   SELECT userid, 
--          event_created_at,
--          amount_in_dollars
--   FROM loan_funded) a
--   JOIN exp_user ON a.userid  = exp_user.userid 
--     AND a.event_created_at >= exp_user.min_exp_timestamp 
--   GROUP BY 1
-- )

, final_table AS (
  SELECT 
    a.assignment_group,
    uniq_email_delivered,
    uniq_email_opened,
    uniq_email_clicked,
    even_uniq_view,
    even_uniq_click,
    enter_even_flow,
    users_submit_api as submit_api,
    users_receive_offer as receive_loan_offer,
    users_click_loan_offer as click_loan_offer,
--     users_click_special_offer,
    even_loan_uniq_conv as convert_loan_offer,
    even_loan_uniq_conv*1.0/users_submit_api as funded_to_applied_rate
--     even_specialoff_uniq_conv as user_convert_special_offer,
  
--     (users_submit_api * 1.00 / enter_even_flow) AS api_submit_rate,
--     (users_receive_loan_offer * 1.00 / users_submit_api) AS receive_offer_rate,
--     (users_click_loan_offer * 1.00 / users_receive_loan_offer) AS click_loan_offer_rate,
--     (user_convert_loan_offer * 1.00 / users_click_loan_offer) AS convert_loan_offer_rate
--     (user_convert_special_offer * 1.00 / users_click_special_offer) AS convert_special_offer_rate
  FROM total_email_engagement a
  LEFT JOIN total_engagement b on a.assignment_group = b.assignment_group
  LEFT JOIN mktp_engagement c ON a.assignment_group = c.assignment_group
  LEFT JOIN even_engagement d ON a.assignment_group = d.assignment_group
  LEFT JOIN enter_even_flow e ON a.assignment_group = e.assignment_group
  LEFT JOIN submit_even_api f ON a.assignment_group = f.assignment_group
  LEFT JOIN even_conversion g ON a.assignment_group = g.assignment_group
  LEFT JOIN other_mktp_conversion h ON a.assignment_group = h.assignment_group
  LEFT JOIN user_offered_loans i ON a.assignment_group = i.assignment_group
  LEFT JOIN user_clicked_mpl_offers j ON a.assignment_group = j.assignment_group
  ORDER BY 1
)

select * from final_table