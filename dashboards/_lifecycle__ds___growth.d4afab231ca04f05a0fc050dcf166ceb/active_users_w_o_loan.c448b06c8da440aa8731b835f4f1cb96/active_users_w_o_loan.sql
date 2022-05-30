WITH investment AS (SELECT userid, investment_balance
                    FROM (SELECT userid 
                               , cash_cashbalance + equity_equityvalue as investment_balance
                               , createdon
                               , ROW_NUMBER() OVER (PARTITION BY userid ORDER BY createdon DESC) AS rank
                          FROM ml_public.drivewealthdaily_snapshot
                          -- https://moneylion.atlassian.net/wiki/spaces/EA/pages/771915801/Investment+Account+Data+Drivewealth+-+Redshift
                          WHERE [updatedon:day] >= [getdate():day] - 5 -- recent snapshot
                                AND triggeredby = 'pluscron') 
                    WHERE rank = 1 and investment_balance > 500)

, core_plus_users AS (SELECT a.userid, 
                             a.status, 
                             doc_type as membership,
                             addresses_state as state,
                             email, 
                             name_first as first_name,
                             name_last as last_name
--                              employments_averagemonthlyincome as estimated_monthly_income
                      FROM ml_public.subscription a
                      LEFT JOIN ml_public.usercollection b on a.userid = b.userid
                      WHERE doc_type IN ('ML-Free-Heavy','ML-Plus','CB-Plus') 
                            AND a.status = 'active' AND b.status = 'active'
                            AND a.activeon >= '2017-12-04') -- avoid testing accts prior to plus release

    , loan_status AS (SELECT userid, loanid, isfunded, loanstatus
                      FROM ml_finance.fpall_ll
                      WHERE isfunded = 1 and loanstatus <> 'Paid Off Loan')

SELECT count(DISTINCT userid)
FROM (SELECT b.*, 
             a.investment_balance,
             c.loanid, 
             isfunded, 
             c.loanstatus 
      FROM investment a
      INNER JOIN core_plus_users b ON a.userid = b.userid
      LEFT JOIN loan_status c ON a.userid = c.userid
      WHERE isfunded IS NULL AND b.userid NOT IN [MLUserIds]
      ORDER BY membership, state, investment_balance)