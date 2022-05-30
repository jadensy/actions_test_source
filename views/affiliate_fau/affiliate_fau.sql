WITH payout_plan AS (
  select 'steady' as media_agency, 'RM funded' as event, 10 as amount
  union all
  select 'even' as media_agency, 'IC disbursed' as event, 10 as amount
  union all
  select 'even' as media_agency, 'CB+ disbursed' as event, 20 as amount
  union all
  select 'nfp' as media_agency, 'IC disbursed' as event, 10 as amount
  union all
  select 'nfp' as media_agency, 'RM funded' as event, 5 as amount
  union all
  select 'nfp' as media_agency, 'RM funded with DD' as event, 25 as amount
  union all
  select 'nfp' as media_agency, 'CB+ disbursed' as event, 10 as amount
  where [media_agency=media_agency])

, overall AS (
  select userid, email, touchtime, product_su, product_su_datetime, media_agency, campaign
  from marketing.fct_attribution_model__product_last_touch 
  where source='impact'
  and [media_agency=media_agency])

-------------- Lite, Core & FFA SU-------------------
, lite_account AS (
  select userid, email, media_agency, campaign,
  product_su as lite_account,
  product_su_datetime as lite_created_datetime
  from overall 
  where product_su = 'lite'
  order by email)

, lite_to_core_account AS (
  select userid, email, media_agency, campaign,
  product_su as core_account,
  product_su_datetime as core_su_datetime
  from overall 
  where email in (select email from overall where product_su = 'lite') 
  and product_su = 'new core'
  order by email)

, core_to_ffa_account AS (
  select userid, email, media_agency, 
  product_su as ffa_product,
  product_su_datetime as ffa_su_datetime
  from overall 
  where email in (select email from overall where product_su = 'lite') 
  and product_su not in ('lite', 'new core')
  order by email)

, user_su_summary AS (
  select a.*, 
  b.core_account, b.core_su_datetime,
  c.ffa_product, c.ffa_su_datetime
  from lite_account a
  left join lite_to_core_account b
  on (a.email=b.email 
      and b.core_su_datetime between a.lite_created_datetime and a.lite_created_datetime+30)
  left join core_to_ffa_account c
  on (a.email=c.email
      and c.ffa_su_datetime between a.lite_created_datetime and a.lite_created_datetime+30))

-------------- FFA Activation-------------------

, ic_users_disbursed as (
  select  
    a.user_id as userid,
    b.email,
    min(date(a.created_at)) as first_date,
    min(a.created_at) as first_timestamp
  from cashadvance_marketing.fct_first_ic_taken a
  join lion1.user b on a.user_id =_id
  where b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)    
  group by 1,2  
)

, cb_users_disbursed as (
  select
    a.userid,
    b.email,
    min(date(c.successdate)) as first_date,
    min(c.successdate) as first_timestamp
  from ml_finance.fpall_ll a 
  join lion1.user b on a.userid = _id 
  join ml_public.loanapp_originationhistories_ll c on a.loanid = c.loanid
  where isfunded = 1 
    and b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
  group by 1,2
  having min(date(successdate)) is not null)

, rm_users_first_deposit as (
  select a.ml_user_id as userid, b.email, 
    min(date(a.timestamp)) as first_date,
    min(a.timestamp) as first_timestamp
  from dda2.fct_deposit a
  left join lion1.user b on a.ml_user_id = b._id
  where true
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
    and a.amount > 1
  group by 1,2
)

, rm_users_first_dd as (
  select a.user_id, b.email, 
    min(date(a.txn_created_on)) as first_date,
    min(a.txn_created_on) as first_timestamp
  from bvenrichment.base_transaction_enriched a
  left join lion1.user b on a.user_id = b._id
  where true 
    and account_name = 'RoarMoney'
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
    and pcd4ca_tagger is not null
  group by 1,2)

, user_su_activation_summary AS (
  select a.*,
  b.first_timestamp as first_ic_disb_dt,
  c.first_timestamp as first_cb_disb_dt,
  d.first_timestamp as first_rm_depo_dt,
  e.first_timestamp as first_dd_dt
  from user_su_summary a
  left join ic_users_disbursed b
  on (a.email=b.email 
      and b.first_timestamp between a.lite_created_datetime and a.lite_created_datetime+30)
  left join cb_users_disbursed c
  on (a.email=c.email 
      and c.first_timestamp between a.lite_created_datetime and a.lite_created_datetime+30)
  left join rm_users_first_deposit d 
  on (a.email=d.email 
      and d.first_timestamp between a.lite_created_datetime and a.lite_created_datetime+30)
  left join rm_users_first_dd e
  on (a.email=e.email 
      and e.first_timestamp between a.lite_created_datetime and a.lite_created_datetime+60))

, activated_user AS (
  select userid, email, media_agency, campaign, ffa_product, 
  first_ic_disb_dt as activation_date, 'IC disbursed' as event
  from user_su_activation_summary
  where first_ic_disb_dt is not null
  union all 
  select userid, email, media_agency, campaign, ffa_product, 
  first_cb_disb_dt as activation_date, 'CB+ disbursed' as event
  from user_su_activation_summary
  where first_cb_disb_dt is not null
  union all
  select userid, email, media_agency, campaign, ffa_product, 
  first_rm_depo_dt as activation_date, 'RM funded' as event
  from user_su_activation_summary
  where first_rm_depo_dt is not null
  union all 
  select userid, email, media_agency, campaign, ffa_product, 
  first_dd_dt as activation_date, 'RM funded with DD' as event
  from user_su_activation_summary
  where first_dd_dt is not null)

, temp_fau AS (
  select a.*, case when b.amount is null then 0 else b.amount end as amount
  from activated_user a
  left join payout_plan b 
  on a.media_agency=b.media_agency and a.event=b.event)

, total_fau AS (
  select *
  from (select *,
        row_number() over(partition by email order by amount desc, activation_date asc) as rn
        from temp_fau)
  where rn=1)

select email, ffa_product, activation_date, event 
from total_fau