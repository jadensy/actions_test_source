WITH fte_old AS (
  SELECT transaction_id
  , user_id
  , MAX(transaction_id_extended) AS transaction_id_extended
  , MIN(created_at) AS transfer_ts
  , MAX(CASE WHEN state = 'ERROR' THEN description END) AS error
  , MAX(amount) AS amount_request
  , MAX(DISTINCT CASE WHEN state IN ('PENDING', 'SETTLED') THEN amount END) AS amount_initiated
  FROM (SELECT a.transaction_id  -- txn binds all steps
        , a.user_id
        , a.created_at
        , a.amount
        , a.source_id
--         , c.type AS source_type
--         , a.destination_id
--         , b.type AS destination_type
        , a.state
        , a.transaction_id_extended
        , a.description
        FROM  ml_public.postgres_fund_transfer_event a 
        LEFT JOIN wallet.fund_option b
        ON a.destination_id = b.id
        LEFT JOIN wallet.fund_option c
        ON a.source_id = c.id
        WHERE c.type = 'ACH'
        AND b.type = 'ML_INVESTMENT'
        AND a.created_at >= '2021-01-01'
       )
  GROUP BY 1,2
  )

, fte_new AS (
  SELECT external_unique_transaction_id AS transaction_id
  , user_id
  , '' AS transaction_id_extended
  , created_at AS transfer_ts
  , CASE WHEN a.error_code IS NOT NULL THEN description END AS error
  , a.amount AS amount_request
  , CASE WHEN a.status IN ('PENDING', 'SUCCESS') THEN a.amount END AS amount_initiated
  FROM wallet.fund_transfer a
  LEFT JOIN paymentplatform_raw.transfer_steps b
  ON a.transfer_id = b.transfer_id
  WHERE a.type = 'ACH_TO_ML_INVESTMENT'
  )

, fte_base AS (
  SELECT * FROM fte_old
  UNION
  SELECT * FROM fte_new
  )

, return_base AS (
  SELECT createdon
  , drivewealthaccountid
  , _id
  , amount
  , depositid
  , comment_date
  , return_code
  , balance_after_return
  FROM (SELECT createdon
        , drivewealthaccountid
        , _id
        , - tranamount AS amount
        , CASE WHEN accountbalance < 0 THEN 'Negative' ELSE 'Positive' END AS balance_after_return
        , SPLIT_PART(TRIM(SPLIT_PART(comment,'Reason:', 1)),' - ', 2) AS depositid
--         , CASE
--             WHEN depositid LIKE 'DW%' THEN SPLIT_PART(comment,'Reason:', 2)
--             ELSE SPLIT_PART(comment,'-', 2) 
--           END AS return_code
        , CASE 
            WHEN lower(comment) ~ '((r10)|(advise))' THEN 'a. R10 - Customer Advises Unauthorized'
            WHEN lower(comment) ~ '((r08)|(payment stop))' THEN 'b. R08 - Payment Stopped'
            WHEN lower(comment) ~ '((r01)|(insufficient))' THEN 'c. R01 - Insufficient Funds'
            WHEN lower(comment) ~ '((webhook))' THEN 'd. Webhook Issue - Moved to Successful'
            WHEN lower(comment) ~ '((ml acct close)|(acct frzn)|(locate)|(r16)|(invalid)|(close))' 
              THEN 'e. ML Account Closed/Cannot Locate Account/Frozen/Invalid Acct Number'
            WHEN lower(comment) ~ '((r20)|(non transaction ac))' THEN 'f. Non-txn Account'
            WHEN lower(comment) ~ '((unknown)|(not auth))' THEN 'g. Unauthorized'
            WHEN lower(comment) ~ '((r09))' THEN 'h. R09 - Uncollected Funds'
            WHEN lower(comment) ~ '((r17))' THEN 'i. R17 - File Record Edit Criteria'
            WHEN (lower(comment) ~ '((ach deposit return))' AND
                  lower(comment) !~ '((r10)|(insufficient funds)|(advise)|(r08)|(payment stop)|(r01)|(webhook)|(ml acct close)|
                  (acct frzn)|(locate)|(r16)|(invalid)|(close)|(r20)|(non transaction ac)|(unknown)|(not auth)|(r09)|(r17))')
              THEN 'j. ACH Deposit Return Without Stated Reason'
            ELSE 'k. Others'
          END AS return_code
        , CASE
            WHEN comment LIKE '%FROM%' THEN SPLIT_PART(SPLIT_PART(TRIM(SPLIT_PART(comment,'FROM ', 2)),'-', 1), ' ',1)
            ELSE SPLIT_PART(SPLIT_PART(TRIM(SPLIT_PART(comment,'RETURN ', 2)),'-', 1), ' ',1)
          END AS part_1 -- get date text
        , CASE 
            WHEN CHARINDEX('.', part_1) = 4 THEN LEFT(part_1,1) || '.' || RIGHT(part_1, LEN(part_1)-1)
            WHEN CHARINDEX('.', part_1) = 5 THEN LEFT(part_1,2) || '.' || RIGHT(part_1, LEN(part_1)-2)
            ELSE part_1 -- fix incorrect date text
          END AS part_2
        , CASE
            WHEN SPLIT_PART(part_2,'.',3) <> '' THEN part_2
            WHEN SPLIT_PART(part_2,'.',1) >= DATEPART(month, createdon) 
              THEN part_2 || '.' || RIGHT(DATEPART(year, createdon) - 1,2)::text
            ELSE part_2 || '.' || RIGHT(DATEPART(year, createdon),2)::text
          END AS part_3 -- fix incomplete year
        , TO_DATE(part_3, 'MM.DD.YY') AS comment_date
        , comment_date > '2020-12-01' AS date_check
        FROM wealth_raw.drivewealthaccounttransaction
        WHERE tranamount < 0 AND CHARINDEX('ach ', LOWER(comment)) AND CHARINDEX('return', LOWER(comment))
        AND createdon >= '2020-12-01')
  WHERE date_check = true
  )

, wealth_base_1 AS (
  SELECT a.transactionid
  , a.depositid
  , a.drivewealthaccountid
  , a.createdon AS success_ts
  , a.amount AS amount_success
  , a.status 
  , b.comment_date
--   , b._id as rtn_id
  , b.createdon AS return_ts
  , b.amount AS amount_return
  , b.return_code
  , b.balance_after_return
  FROM (
    SELECT * 
    FROM wealth_raw.investmentaccountdeposits
    WHERE createdon >= '2020-12-01'
    AND UPPER(status) NOT IN ('PROCESSING_ERROR', 'FAILED', 'APPROVED')
    AND depositid IN (SELECT depositid FROM return_base)
    ) a
  LEFT JOIN return_base b
  ON a.status NOT IN ('PROCESSING_ERROR', 'FAILED', 'APPROVED')
  AND a.depositid = b.depositid
  )

, wealth_base_2 AS (
  SELECT a.transactionid
  , a.depositid
  , a.drivewealthaccountid
  , a.createdon AS success_ts
  , a.amount AS amount_success
  , a.status 
  , b.comment_date
--   , b._id as rtn_id
  , b.createdon AS return_ts
  , b.amount AS amount_return
  , b.return_code
  , b.balance_after_return
  FROM (
    SELECT * 
    , row_number() OVER (PARTITION BY drivewealthaccountid, amount ORDER BY createdon) AS rn
    FROM wealth_raw.investmentaccountdeposits
    WHERE createdon >= '2020-12-01'
    AND UPPER(status) NOT IN ('PROCESSING_ERROR', 'FAILED', 'APPROVED')
    AND depositid NOT IN (SELECT depositid FROM return_base WHERE depositid <> '')
    ) a
  LEFT JOIN (SELECT * 
             , row_number() OVER (PARTITION BY drivewealthaccountid, amount ORDER BY comment_date) AS rn
             FROM return_base WHERE depositid = '') b
  ON a.drivewealthaccountid = b.drivewealthaccountid
  AND a.amount = b.amount
  AND a.rn = b.rn
  )

, wealth_base AS (
  SELECT * FROM wealth_base_1
  UNION
  SELECT * FROM wealth_base_2
  )

, base AS (
  SELECT a.*
  , success_ts
  , amount_success
  , return_ts
  , amount_return
  , balance_after_return
  , return_code
  , CASE WHEN transaction_id_extended IN (SELECT paymentid FROM 
                                          ml_public.autoinvestdeposithistory) THEN true ELSE false END AS is_auto_invest
  FROM fte_base a
  LEFT JOIN wealth_base b
  ON a.transaction_id = b.transactionid
  WHERE transfer_ts < [current_date:day] -- wealth records might not updated yet
--   AND transfer_ts >= '2021-01-01'
--   AND transfer_ts >= DATEADD('month', -3, [current_date:month])
  )

, main AS (
  SELECT *
  , CASE 
      WHEN error IN ('No user found') THEN 'a.internal_error'
      WHEN error IN ('MFA invalid', 'Failed to challenge MFA') THEN 'b.failed_MFA'
      WHEN lower(error) LIKE '%identity%' THEN 'c.failed_IDT'
      WHEN error IN ('BV2 INFOACCOUNT PRODUCT BALANCE not available', 
                     'BV2 INFOACCOUNT return duplicate result of External Account and unmatchable account type.',
                     'BV2 INFOACCOUNTBALANCE indicate that user need to relink their External Account.',
                     'BV2 INFOACCOUNT return 0 result of External Account.')THEN 'd.failed_get_account'
      WHEN error IN ('BV2 INFOACCOUNTBALANCE return 0 result of External Account''s availableBalance.',
                     'BV2 INFOACCOUNTBALANCE return duplicate result of External Account''s availableBalance.',
                     'BV2 INFOACCOUNTBALANCE return External Account''s availableBalance as null.', 
                     'BV2 API Internal Error', 
                     'Insufficient funds available in ACH account') THEN 'e.failed_balance_call'
      WHEN error LIKE 'First 30 days%' OR error LIKE 'Daily limit%' THEN 'f.limit_reached'
      WHEN amount_initiated IS NULL THEN 'g.failed_initiate'
    END AS error_waterfall
  , CASE
      WHEN error IN ('MFA invalid', 'Failed to challenge MFA', 'Identity untrusted',
                     'Insufficient funds available in ACH account') THEN 'b.user'
      WHEN error LIKE 'First 30 days%' OR error LIKE 'Daily limit%' THEN 'c.fraud'
      WHEN amount_initiated IS NULL THEN 'a.others'
    END AS error_source
  FROM base
  )

, final AS (
  SELECT transaction_id
  , user_id
  , is_auto_invest
  , amount_request
  , transfer_ts
  , CASE WHEN amount_initiated IS NOT NULL THEN True ELSE False END AS initiated
  , amount_initiated
  , success_ts
  , CASE 
      WHEN amount_initiated IS NOT NULL AND amount_success IS NOT NULL THEN True
      WHEN amount_initiated IS NOT NULL AND amount_success IS NULL THEN False 
    END AS success
  , amount_success
  , return_ts
  , CASE 
      WHEN amount_success IS NOT NULL AND amount_return IS NOT NULL THEN True 
      WHEN amount_success IS NOT NULL AND amount_return IS NULL THEN False 
    END AS returned
  , amount_return
  , balance_after_return
  , error_waterfall
  , error_source
  , error
  , return_code
--   , CASE
--       WHEN error LIKE '%https://plusapi.moneylion.com/v2/deposit%' THEN 'DEPOSIT TIMEOUT'
--       ELSE error
--     END AS error
  FROM main
  )

SELECT *
FROM final
WHERE [transfer_ts:week]<[current_date:day] - 5