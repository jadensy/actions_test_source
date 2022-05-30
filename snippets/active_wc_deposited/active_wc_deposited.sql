WITH adm_fee_table AS (SELECT checking_account_id, sum(amount) as admin_fee
                       FROM dda2.fct_posted_transaction
                       WHERE type ~* 'fee' AND description = 'Administrative Fee'
                         AND ml_user_id NOT IN (SELECT userid FROM [ml_emp_ids])
                       GROUP BY 1)

--- Group into active and cancel
--- Group into existing and new
--- Group into after and within promo
--- Group into waived and unwaived
--- Only considered users who made at least 1 purchase and deposit
, active_cancel_group AS (SELECT a.checking_account_id
                               , a.ml_user_id
                               , a.virtual_account_status as acct_status
                               , enrollment_date
                               , first_deposit_date
                               , first_purchase_date
                               , admin_fee
                               , CASE WHEN a.ml_user_id IN (SELECT ml_user_id FROM ml_public.postgres_transactions_mv)
                                      THEN 'Existing' ELSE 'New' END as dda
                               , CASE WHEN enrollment_date >= '2020-08-01' THEN 'after promo' 
                                      ELSE 'within promo' END as join_category
                               , CASE WHEN first_purchase_date IS NOT NULL OR first_deposit_date IS NOT NULL THEN 1
                                      ELSE 0 END as ind -- only considered users who made at least 1 purchase and deposit
                               , CASE WHEN admin_fee >= 0 THEN 'Waived' 
                                      WHEN admin_fee < 0 THEN 'Not waived'
                                      WHEN admin_fee IS NULL AND ind = 1 THEN 'Waived'
                                      END as category
                               ---, DATEDIFF('day', enrollment_date, first_deposit_date) as daydiff
                          FROM dda2.fct_user_summary a
                          LEFT JOIN adm_fee_table b ON a.checking_account_id = b.checking_account_id
                          WHERE enrollment_date < '2021-02-01'),


---- Filter the required target group who at least performed a deposit
filtered_1 AS
(SELECT checking_account_id,
        ml_user_id,
        first_deposit_date,
        first_purchase_date,
        DATEDIFF('day', first_deposit_date,first_purchase_date) as daydiff,
        CASE WHEN dda = 'Existing' AND join_category = 'within promo' AND  category = 'Waived' THEN 'Waived Accounts'
             WHEN dda = 'New' AND join_category = 'within promo' AND  category = 'Waived' THEN 'Waived Accounts'
             WHEN dda = 'Existing' AND join_category = 'after promo' AND  category = 'Not waived' THEN 'Charged Accounts'
             WHEN dda = 'New' AND join_category = 'after promo' AND  category = 'Not waived' THEN 'Charged Accounts'
             ELSE NULL
        END AS category
FROM active_cancel_group
WHERE acct_status = 'Active' 
AND first_deposit_date IS NOT NULL),


SELECT * FROM filtered_1
WHERE category IS NOT NULL