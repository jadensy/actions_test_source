WITH all_members AS (SELECT DISTINCT userid
                     FROM ml_public.subscription
                     WHERE doc_type IN ('ML-Free-Heavy', 'ML-Plus', 'CB-Plus')
                       AND subtype != 'ReturningOOS'
                       AND status NOT IN ('pending', 'deactivated')
                       AND activeon >= '2017-12-04')

, wealth AS (SELECT a.userid, c.accountstatus, c.accountno
                FROM ml_public.drivewealthuser a
                LEFT JOIN ml_public.drivewealth_accountuser b
                       ON a._id = b.drivewealthuserid
                LEFT JOIN ml_public.drivewealth_account c
                       ON b.drivewealthaccountid = c._id
                WHERE c.accountstatus <> 'PENDING' AND c.accountstatus <> 'CLOSED'
                  AND c.accountid != ''
                  AND c.internaltype = 'INVESTMENT'
                  AND userid IN (SELECT userid FROM all_members))

, wealth_bal AS (SELECT userid, accountno, account_balance, createdon
                 FROM (SELECT userid, 
                              accountno,
                              cash_cashbalance + equity_equityvalue as account_balance,
                              createdon,
                              ROW_NUMBER() OVER (PARTITION BY userid ORDER BY createdon DESC) as rank
                       FROM ml_public.drivewealthdaily_snapshot
                       WHERE triggeredby = 'pluscron' )
                 WHERE rank = 1  )

, email AS (SELECT DISTINCT userid, email
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY updatedon DESC) as rank
                  FROM ml_public.usercollection
                  WHERE brand = 'ml' )
            WHERE rank = 1)

SELECT a.userid, b.email, a.accountno, a.account_balance 
FROM wealth_bal a
LEFT JOIN email b ON a.userid = b.userid
WHERE account_balance > 700 AND account_balance < 1000
  AND accountno IN (SELECT accountno FROM wealth)
ORDER BY account_balance DESC