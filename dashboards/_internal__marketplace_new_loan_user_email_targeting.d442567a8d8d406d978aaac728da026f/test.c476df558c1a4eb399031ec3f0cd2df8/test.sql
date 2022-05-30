WITH core_users AS (
  SELECT userid, email, first_timestamp, 'core' AS user_type
  FROM marketing.stg_user__new_core
),
mapping AS (
  SELECT _id AS moneylionuserid, 
         email 
  FROM lion1.user 
  WHERE brand = 'ml' 
  AND deleted <> 1  
),
enter_even_flow AS (
  SELECT *
  FROM (
    SELECT user_id, "timestamp" AS event_timestamp
    FROM ios.loan_mpl_customize_loan_view

    UNION ALL

    SELECT user_id, "timestamp" AS event_timestamp  
    FROM android.loan_mpl_customize_loan_view
  )
),
user_enter_even_flow AS (
  SELECT user_type, COUNT(DISTINCT user_id) AS a_enter_flow
  FROM (
  SELECT a.user_id, 
         event_timestamp,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM enter_even_flow a
  LEFT JOIN core_users b ON user_id = b.email AND event_timestamp >= first_timestamp 
  )
  GROUP BY 1
),
mpl_users AS (
  SELECT *
  FROM (
    SELECT user_id, "timestamp" AS event_timestamp
    FROM ios.loan_mpl_last_thing_see_offers

    UNION ALL

    SELECT user_id, "timestamp" AS event_timestamp  
    FROM android.loan_mpl_last_thing_see_offers
  )
),
mpl_user_submit_api AS (
  SELECT user_type, COUNT(DISTINCT user_id) AS b_submit_api
  FROM (
  SELECT a.user_id, 
         event_timestamp,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM mpl_users a
  LEFT JOIN core_users b ON user_id = b.email AND event_timestamp >= first_timestamp 
  )
  GROUP BY 1
),
even_loan_offers AS (
  SELECT userid, 
         created_at
  FROM marketplace.fct_even_lender_offers
  GROUP BY 1,2
),
user_offered_loans AS (
  SELECT user_type, COUNT(DISTINCT userid) AS c_loan_offered
  FROM (
  SELECT a.userid,
         created_at,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM even_loan_offers a
  LEFT JOIN core_users b ON a.userid = b.userid AND created_at >= first_timestamp
  )
  GROUP BY 1
),
user_clicked_events AS (
  SELECT userid, event_timestamp
  FROM marketplace.fct_even_lender_engagement_events
  WHERE location <> 'special-offers'
  AND event = 'click'
),
user_clicked_mpl_offers AS (
  SELECT user_type, COUNT(DISTINCT userid) AS d_offer_clicked
  FROM (
  SELECT a.userid, 
         event_timestamp,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM user_clicked_events a
  LEFT JOIN core_users b ON a.userid = b.userid AND event_timestamp >= first_timestamp 
  )
  GROUP BY 1
),
loan_applied AS (
  SELECT lead_id, 
         userid,
         event_created_at
  FROM marketplace.fct_even_lead_events
  WHERE event_type = 'applied'
),
user_applied_mpl_offers AS (
  SELECT user_type, COUNT(DISTINCT userid) AS e_offer_applied
  FROM (
  SELECT a.userid, 
         event_created_at,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM loan_applied a
  LEFT JOIN core_users b ON a.userid = b.userid AND event_created_at >= first_timestamp
  )
  GROUP BY 1
),
loan_funded AS (
  SELECT lead_id, 
         userid,
         event_created_at,
         amount_in_dollars
  FROM marketplace.fct_even_lead_events
  WHERE event_type = 'funded'
),
user_funded_mpl_offers AS (
  SELECT user_type, COUNT(DISTINCT userid) AS f_offer_funded
  FROM (
  SELECT a.userid, 
         event_created_at,
         amount_in_dollars,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM loan_funded a
  LEFT JOIN core_users b ON a.userid = b.userid AND event_created_at >= first_timestamp 
  )
  GROUP BY 1
),
loan_payout AS (
  SELECT lead_id, 
         userid,
         booked_at,
         payout_dollar,
         type
  FROM marketplace.fct_even_conversion_revenue
),
user_payout_received AS (
  SELECT user_type, COUNT(DISTINCT userid) AS g_payout_received
  FROM (
  SELECT a.userid, 
         booked_at,
         payout_dollar,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM loan_payout a
  LEFT JOIN core_users b ON a.userid = b.userid AND booked_at >= first_timestamp 
  )
  GROUP BY 1
),
even_loan_payout_received AS (
  SELECT user_type, COUNT(DISTINCT userid) AS h_even_loan_payout
  FROM (
  SELECT a.userid, 
         booked_at,
         payout_dollar,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM loan_payout a
  LEFT JOIN core_users b ON a.userid = b.userid AND booked_at >= first_timestamp 
  WHERE type = 'loan'
  )
  GROUP BY 1
),
even_specialoffer_payout_received AS (
  SELECT user_type, COUNT(DISTINCT userid) AS i_specialoffer_payout
  FROM (
  SELECT a.userid, 
         booked_at,
         payout_dollar,
         CASE WHEN user_type IS NULL THEN 'lite'
              ELSE 'core' END AS user_type
  FROM loan_payout a
  LEFT JOIN core_users b ON a.userid = b.userid AND booked_at >= first_timestamp 
  WHERE type = 'specialOffer'
  )
  GROUP BY 1
),
final_table AS (
  SELECT a.user_type,
         a_enter_flow,
         b_submit_api,
         c_loan_offered,
         d_offer_clicked,
         e_offer_applied,
         f_offer_funded,
         g_payout_received,
         h_even_loan_payout,
         i_specialoffer_payout
  FROM user_enter_even_flow a
  LEFT JOIN mpl_user_submit_api c ON a.user_type = c.user_type
  LEFT JOIN user_offered_loans d ON a.user_type = d.user_type
  LEFT JOIN user_clicked_mpl_offers e ON a.user_type = e.user_type
  LEFT JOIN user_applied_mpl_offers f ON a.user_type = f.user_type
  LEFT JOIN user_funded_mpl_offers g ON a.user_type = g.user_type
  LEFT JOIN user_payout_received h ON a.user_type = h.user_type
  LEFT JOIN even_loan_payout_received i ON a.user_type = i.user_type
  LEFT JOIN even_specialoffer_payout_received j ON a.user_type = j.user_type
),
final_incl_total AS (
  SELECT * 
  FROM (
    SELECT user_type,
           a_enter_flow,
           b_submit_api,
           c_loan_offered,
           d_offer_clicked,
           e_offer_applied,
           f_offer_funded,
           g_payout_received,
           h_even_loan_payout,
           i_specialoffer_payout
    FROM final_table
    UNION
    (SELECT 'total' AS user_type,
            SUM(a_enter_flow) AS a_enter_flow,
            SUM(b_submit_api) AS b_submit_api,
            SUM(c_loan_offered) AS c_loan_offered,
            SUM(d_offer_clicked) AS d_offer_clicked,
            SUM(e_offer_applied) AS e_offer_applied,
            SUM(f_offer_funded) AS f_offer_funded,
            SUM(g_payout_received) AS g_payout_received,
            SUM(h_even_loan_payout) AS h_even_loan_payout,
            SUM(i_specialoffer_payout) AS i_specialoffer_payout
     FROM final_table
     )
  )
),
melted_table AS (
SELECT user_type, event, value
FROM final_incl_total
unpivot
(
  value
  FOR event IN ([a_enter_flow], [b_submit_api], [c_loan_offered], [d_offer_clicked], [e_offer_applied], [f_offer_funded], [g_payout_received], [h_even_loan_payout], [i_specialoffer_payout])
) 
)
  
SELECT user_type, 
       event, 
       value, 
       lag(value) OVER (PARTITION BY user_type ORDER BY event ASC) AS prev_value,
       ((CASE WHEN event NOT IN ('h_even_loan_payout', 'i_specialoffer_payout') THEN value ELSE NULL END) * 1.00 / prev_value) AS stepwise_pct_retained
FROM melted_table
ORDER BY 2 ASC