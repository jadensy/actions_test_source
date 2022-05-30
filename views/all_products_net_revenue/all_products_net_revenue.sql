with combined as (
  select
  userid,
  dt,
  transaction_type,
  instacash_net_rev,
  membership_fee,
  dda_net_revenue as dda_transc_gross_profit,
  cb_net_revenue_for_dt,
  rm_net_revenue as rm_gross_revenue,
  wealth_net_revenue,
  --0.00 as marketplace_rev
  marketplace_net_revenue as marketplace_rev, --started from June 2021 onwards
  crypto_net_revenue -- started from 28 June 2021
  from marketing.fct_ltv_user_level_transaction 
)

select * from combined

/*, mapping AS (
  SELECT _id AS moneylionuserid, 
         email 
  FROM lion1.user 
  WHERE brand = 'ml' 
  AND deleted <> 1
)

, marketplace_rev as (
SELECT userid,
       DATE_TRUNC('day', converted_at) AS dt,
       SUM(revenue) AS marketplace_rev
FROM marketplace.fct_conversion_revenue a
INNER JOIN mapping ON a.userid = mapping.moneylionuserid
WHERE converted_at >= '2021-06-01'
AND revenue <> 0
GROUP BY 1,2
)

, product_rev as (
  select *
  from combined 
  
  union all
  
  select 
  userid,
  dt,
  'marketplace' as transaction_type,
  0.00 as instacash_net_rev,
  0.00 as membership_fee,
  0.00 as dda_transc_gross_profit,
  0.00 as cb_net_revenue_for_dt,
  0.00 as rm_gross_revenue,
  0.00 as wealth_net_revenue,
  marketplace_rev  
  from marketplace_rev
  )

select * from product_rev*/