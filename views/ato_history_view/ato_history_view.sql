WITH reference_query_phone AS (
  SELECT 
    a.claim_date
    , us.createdon AS ato_stopped
    , a.* 
  FROM 
  (
    SELECT 
    uc.email
    , uc.name_first
    , uc.name_last
    , uc.userid
    , uc.status
    , uc.updatedon
    , uc.contacts_mobilenum            
    , uc.createdon AS account_creation_date
    , account_status AS rm_account_status 
    , CASE WHEN result IN ('HARD_REJECT', 'SOFT_REJECT') THEN 'failed IDV'
           WHEN result = 'PASSED' THEN 'pass IDV'
           ELSE 'no IDV attempt' 
      END AS idv_result
    , CASE WHEN u.last4 > 0 THEN 'ssn exists' 
           ELSE 'lite' 
      END AS account_type
    , ut.endpoint
    , min(created) AS RM_created_date
    , min(mv.created_on) AS first_IDV_date
    , min(ut.createdon) AS claim_date 
    , date_trunc('week', uc.createdon) AS account_creation_week
    , date_trunc('week', min(mv.created_on)) AS IDV_week
    , date_trunc('week', min(ut.createdon)) AS claim_week
    , DATEDIFF ( day,  min(ut.createdon), min(mv.created_on)) AS idv_claim_diff
    , DATEDIFF ( day,  min(ut.createdon), uc.createdon) AS account_creation_claim_diff
    FROM ml_public.usertransaction_v3 ut
    LEFT JOIN ml_public.usercollection uc ON ut.userid = uc.userid
    LEFT JOIN lion1.user u ON u._id = uc.userid
    LEFT JOIN membership.verification mv ON mv.user_id = uc.userid
    LEFT JOIN dda2_raw.account dda2a ON dda2a.ml_user_id = uc.userid AND account_type = 'Virtual'
    WHERE  ut.createdon > '2021-05-01' AND appname = 'dashboardApi' AND endpoint LIKE '%claim%' 
    AND ( date_trunc('week',uc.createdon) < date_trunc('week',ut.createdon)) 
    GROUP BY 1,2, 3,4,5,6,7,8,9,10,11,12
    ) a
    LEFT JOIN 
    (
       SELECT 
        user_id 
        , min(createdon) AS createdon 
       FROM lion1.user_statuses 
       WHERE createdon > '2021-05-01'AND status IN ('suspected_fraud_application', 'suspected_account_takeover')
       GROUP BY user_id 
     ) us ON a.userid = us.user_id AND us.createdon > claim_date
    WHERE account_creation_claim_diff <-1
), reference_query_email AS (
  
  SELECT 
    a.claim_date
    , us.createdon AS ato_stopped
    , a.* 
  FROM 
  (
    SELECT 
        uc.email
      , uc.name_first
      , uc.name_last
      , uc.userid
      , uc.status             
      , uc.updatedon
      ,uc.contacts_mobilenum
      , uc.createdon AS account_creation_date
      , account_status AS rm_account_status
      , CASE WHEN u.last4 > 0 THEN 'ssn exists' 
             ELSE 'lite' 
        END AS account_type
      , min(created) AS RM_created_date
      , min(mv.created_on) AS first_IDV_date
      , min(ut.createdon) AS claim_date 
      , date_trunc('week', uc.createdon) AS account_creation_week
      , date_trunc('week', min(mv.created_on)) AS IDV_week
      , date_trunc('week', min(ut.createdon)) AS claim_week
      , DATEDIFF (day,  min(ut.createdon), min(mv.created_on)) AS idv_claim_diff
      , DATEDIFF (day,  min(ut.createdon), uc.createdon) AS account_creation_claim_diff
    FROM ml_public.usertransaction_v3 ut
    LEFT JOIN ml_public.usercollection uc ON ut.userid = uc.userid
    LEFT JOIN lion1.user u ON u._id = uc.userid
    LEFT JOIN membership.verification mv ON mv.user_id = uc.userid
    LEFT JOIN dda2_raw.account dda2a ON dda2a.ml_user_id = uc.userid AND account_type = 'Virtual'
    WHERE   endpointmethod = 'PUT ' 
    AND appname NOT LIKE '%mobile%' 
    AND ut.createdon > '2021-07-15'
    AND referer NOT LIKE '%signup%' 
    AND req LIKE '%email%' 
    AND httpstatus = '200'
    GROUP BY 1,2, 3,4,5,6,7,8,9,10  
  ) a
  LEFT JOIN 
  (
    SELECT   
      user_id 
      , min(createdon) AS createdon 
    FROM lion1.user_statuses 
    WHERE createdon > '2021-05-01'
    AND status IN ('suspected_fraud_application', 'suspected_account_takeover')
    GROUP BY user_id 
  ) us ON a.userid = us.user_id AND us.createdon > claim_date
), claim_phone_uids AS (
  SELECT 
    ut.userid
    , 'phone claim' AS claim_type
    , regexp_replace(ut.req, '[^0-9]+', '') AS feature_value
    , ut.deviceid
    , ut.createdon AS claim_timestamp
    , li.createdon AS status_update_timestamp
    , li.status
  FROM ml_public.usertransaction_v3 ut
  LEFT JOIN lion1.user_statuses li ON ut.userid = li.user_id AND li.createdon > ut.createdon
  WHERE endpoint = '/contact/claim' --endpoint for claim phone
  AND appname = 'dashboardApi' --filter for web claims
  AND httpstatus = '200' --filter for successful endpoint calls only
  ORDER BY userid, status_update_timestamp ASC
  
), claim_email_uids as (
  SELECT 
    ut.userid
    , 'email claim' AS claim_type
    , split_part(replace(ut.req, '"}', ''), '"email":"', 2) AS username
    , ut.deviceid
    , ut.createdon AS claim_timestamp
    , li.createdon AS status_update_timestamp
    , li.status
  FROM ml_public.usertransaction_v3 ut
  LEFT JOIN lion1.user_statuses li ON ut.userid = li.user_id AND li.createdon > ut.createdon
  WHERE endpointmethod = 'PUT' 
  AND appname NOT LIKE '%mobile%' --no mobile, web only
  AND referer NOT LIKE '%signup%' --not signup
  AND req LIKE '%email%' --filter for claim emails only
  AND httpstatus = '200' --filter for successful endpoint calls only
  ORDER BY userid, status_update_timestamp ASC
  
)

SELECT *
FROM
(
  (
     SELECT * FROM claim_phone_uids
  )
  UNION
  (
    SELECT * FROM claim_email_uids
  )
)
WHERE status IN ('suspected_fraud_application', 'suspected_account_takeover')
ORDER BY userid, claim_timestamp