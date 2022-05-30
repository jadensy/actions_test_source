--no-cache
with ctx as (
    select 
        MAX(DATEDIFF('minute', starttime, endtime)) as duration
    from "admin".stl_query_history
    where 1=1
    -- AND starttime >= DATE_TRUNC('day',sysdate) - 7
    AND userid <> 114
	AND userid <> 233
	AND userid <> 169
  AND LOWER(querytxt) LIKE '%select%'
--   and (LOWER(querytxt) LIKE '%mysql_bv_log%'
--        OR LOWER(querytxt) LIKE '%usercollection%'
-- OR LOWER(querytxt) LIKE '%fpall_ll%'
-- OR LOWER(querytxt) LIKE '%mysql_institution_provider%'
-- OR LOWER(querytxt) LIKE '%subscription%'
-- OR LOWER(querytxt) LIKE '%fraud_sheet%'
-- OR LOWER(querytxt) LIKE '%data%'
-- OR LOWER(querytxt) LIKE '%drivewealthdaily_snapshot%'
-- OR LOWER(querytxt) LIKE '%data%'
-- OR LOWER(querytxt) LIKE '%drivewealthuser%'
-- OR LOWER(querytxt) LIKE '%userverification_bvunderwriting%'
-- OR LOWER(querytxt) LIKE '%ddadirect_deposit_confirm%'
-- OR LOWER(querytxt) LIKE '%fivenine_completecallreport%'
-- OR LOWER(querytxt) LIKE '%pendingrewarddetails%'
-- OR LOWER(querytxt) LIKE '%loan_status%'
-- OR LOWER(querytxt) LIKE '%mysql_plaid_link_log%'
-- OR LOWER(querytxt) LIKE '%login%'
-- OR LOWER(querytxt) LIKE '%postgres_fund_option%'
-- OR LOWER(querytxt) LIKE '%mysql_plaid_webhook%'
-- OR LOWER(querytxt) LIKE '%usertransaction_v3%'
-- OR LOWER(querytxt) LIKE '%fpall_ml%'
-- OR LOWER(querytxt) LIKE '%postgres_event_notification_files%'
-- OR LOWER(querytxt) LIKE '%loan_accept_mobile%'
-- OR LOWER(querytxt) LIKE '%postgres_event_notifications%'
-- OR LOWER(querytxt) LIKE '%debit_card_activation_result%'
-- OR LOWER(querytxt) LIKE '%debit_card_activated%'
-- OR LOWER(querytxt) LIKE '%postgres_cash_advance%'
-- OR LOWER(querytxt) LIKE '%points_status%'
-- OR LOWER(querytxt) LIKE '%dda_account_created%'
-- OR LOWER(querytxt) LIKE '%loanapp_loanstatuses_ll%'
-- OR LOWER(querytxt) LIKE '%loanapp_originationhistories_ll%'
-- OR LOWER(querytxt) LIKE '%postgres_product_option%'
-- OR LOWER(querytxt) LIKE '%bvstats%'
-- OR LOWER(querytxt) LIKE '%postgres_processor_option%'
-- OR LOWER(querytxt) LIKE '%screen_viewed%'
-- OR LOWER(querytxt) LIKE '%mysql_bv_institution%'
-- OR LOWER(querytxt) LIKE '%calendar%'
-- OR LOWER(querytxt) LIKE '%user_status%'
-- OR LOWER(querytxt) LIKE '%page_loaded%'
-- OR LOWER(querytxt) LIKE '%referral_status%'
-- OR LOWER(querytxt) LIKE '%referral%'
-- OR LOWER(querytxt) LIKE '%mysql_bv_bank%'
-- OR LOWER(querytxt) LIKE '%messages%'
-- OR LOWER(querytxt) LIKE '%fsexport%'
-- OR LOWER(querytxt) LIKE '%userpointhistories%'
-- OR LOWER(querytxt) LIKE '%shakes%'
-- OR LOWER(querytxt) LIKE '%fpall_rc%'
-- OR LOWER(querytxt) LIKE '%modeltransactions_bvmodel5%'
-- OR LOWER(querytxt) LIKE '%modeltransactions_pcd4ca%'
-- OR LOWER(querytxt) LIKE '%mysql_plaid_balance_log%'
-- OR LOWER(querytxt) LIKE '%email_opened%'
-- OR LOWER(querytxt) LIKE '%subscription_lead%'
-- OR LOWER(querytxt) LIKE '%core_account_created%'
-- OR LOWER(querytxt) LIKE '%subscription_configuration%'
-- OR LOWER(querytxt) LIKE '%application_opened%'
-- OR LOWER(querytxt) LIKE '%bv_plaid_transactions%'
-- OR LOWER(querytxt) LIKE '%bvstats2%'
-- OR LOWER(querytxt) LIKE '%trans_union_credit_information%'
-- OR LOWER(querytxt) LIKE '%insights%'
-- OR LOWER(querytxt) LIKE '%payment_status%'
-- OR LOWER(querytxt) LIKE '%alert_credit_monitoring%'
-- OR LOWER(querytxt) LIKE '%enrollment_status%'
-- OR LOWER(querytxt) LIKE '%plus_referral_status%'
-- OR LOWER(querytxt) LIKE '%email_link_clicked%'
-- OR LOWER(querytxt) LIKE '%postgres_customers%'
-- OR LOWER(querytxt) LIKE '%first_open%'
-- OR LOWER(querytxt) LIKE '%bv_refresh_push_notification%'
-- OR LOWER(querytxt) LIKE '%application_updated%'
-- OR LOWER(querytxt) LIKE '%experiment_viewed%'
-- OR LOWER(querytxt) LIKE '%install_attributed%'
-- OR LOWER(querytxt) LIKE '%first_open%'
-- OR LOWER(querytxt) LIKE '%application_updated%'
-- OR LOWER(querytxt) LIKE '%application_installed%'
-- OR LOWER(querytxt) LIKE '%modeltransactions_pcd4ca_allincomes_transactions%'
-- OR LOWER(querytxt) LIKE '%fivenine_calllogs%'
-- OR LOWER(querytxt) LIKE '%application_updated%'
-- OR LOWER(querytxt) LIKE '%bv_plaid_identity_addresses%'
-- OR LOWER(querytxt) LIKE '%bv_plaid_identity_emails%'
-- OR LOWER(querytxt) LIKE '%bv_plaid_identity_names%'
-- OR LOWER(querytxt) LIKE '%bv_plaid_identity_phonenumbers%'
-- OR LOWER(querytxt) LIKE '%postgres_card_charge_attempts%'
-- OR LOWER(querytxt) LIKE '%screen_viewed%'
-- OR LOWER(querytxt) LIKE '%financial_institution_status%'
-- OR LOWER(querytxt) LIKE '%screen_viewed%'
-- OR LOWER(querytxt) LIKE '%postgres_card_validate_attempts%'
-- OR LOWER(querytxt) LIKE '%credit_simulation%'
-- OR LOWER(querytxt) LIKE '%application_installed%'
-- OR LOWER(querytxt) LIKE '%email_bounced%'
-- OR LOWER(querytxt) LIKE '%drivewealthuser_withdrawals%'
-- OR LOWER(querytxt) LIKE '%declined_push_notifications%'
-- OR LOWER(querytxt) LIKE '%postgres_transactions_mv%'
-- OR LOWER(querytxt) LIKE '%credit_refresh_status%'
-- OR LOWER(querytxt) LIKE '%multifactorauthenticationchallenge%'
-- OR LOWER(querytxt) LIKE '%core_onboarding_idv_initiate_result%'
-- OR LOWER(querytxt) LIKE '%card_status%'
-- OR LOWER(querytxt) LIKE '%backfill_bv5cbcbb_20190620%'
-- OR LOWER(querytxt) LIKE '%fssync%'
-- OR LOWER(querytxt) LIKE '%cm_success%'
-- OR LOWER(querytxt) LIKE '%postgres_fraud_accounts%'
-- OR LOWER(querytxt) LIKE '%usercollection_pii%')
-- 	AND LOWER(querytxt) NOT LIKE '% -- segment%'
-- 	AND LOWER(querytxt) NOT LIKE '%auto analyze%'
-- 	AND LOWER(querytxt) NOT LIKE '%vacuum%'
-- 	AND LOWER(querytxt) NOT LIKE '%small %'
-- 	AND LOWER(querytxt) NOT LIKE '%delete %'
-- 	AND LOWER(querytxt) NOT LIKE '%alter %'
-- 	AND LOWER(querytxt) NOT LIKE '%create %'
-- 	AND LOWER(querytxt) NOT LIKE '%padb_fetch_sample:%'
-- 	AND LOWER(querytxt) NOT LIKE '%copy %'
-- 	AND LOWER(querytxt) NOT LIKE '%insert %'
-- 	AND LOWER(querytxt) NOT LIKE '%update %'
-- 	AND LOWER(querytxt) NOT LIKE '%building %'
-- 	AND LOWER(querytxt) NOT LIKE '%undoing %'
-- 	AND LOWER(querytxt) NOT LIKE '%analyze %'
-- 	AND LOWER(querytxt) NOT LIKE '%fetch %'
    group by querytxt
)
SELECT
	count(1)
  ,CASE 
 when duration >=0 and duration < 60 then duration
--   WHEN duration>=0 AND duration<5 THEN 5
--   WHEN duration>=5 AND duration<10 THEN 10
--   WHEN duration>=10 AND duration<15 THEN 15
--   WHEN duration>=15 AND duration<20 THEN 20
--   WHEN duration>=20 AND duration<25 THEN 25
--   WHEN duration>=25 AND duration<30 THEN 30
--   WHEN duration>=30 AND duration<35 THEN 35
--   WHEN duration>=35 AND duration<40 THEN 40
--   WHEN duration>=40 AND duration<45 THEN 45
--   WHEN duration>=45 AND duration<50 THEN 50
--   WHEN duration>=50 AND duration<55 THEN 55
--   WHEN duration>=55 AND duration<60 THEN 60
  ELSE 61
  END AS duration_for_grouping
-- 	, duration AS duration_for_grouping
FROM ctx
GROUP BY duration_for_grouping
ORDER BY duration_for_grouping