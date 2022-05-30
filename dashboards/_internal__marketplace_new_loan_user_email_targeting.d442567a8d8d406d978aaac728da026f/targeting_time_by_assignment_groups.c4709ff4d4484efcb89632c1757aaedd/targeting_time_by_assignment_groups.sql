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

, final_table AS (
  SELECT 
    distinct 
    CASE 
      WHEN campaign_id = '3894065' THEN 'treatment_1_low_income'
      WHEN campaign_id = '3905910' THEN 'treatment_2_low_bv5_score'
      WHEN campaign_id = '3905916' THEN 'treatment_3_has_liability'
      WHEN campaign_id = '3862445' THEN 'control_leadgen_loan'
    ELSE null END AS assignment_group,
    min_exp_timestamp::date,
    max_exp_timestamp::date
  FROM exp_period
  ORDER BY 1, 2
)

select * from final_table