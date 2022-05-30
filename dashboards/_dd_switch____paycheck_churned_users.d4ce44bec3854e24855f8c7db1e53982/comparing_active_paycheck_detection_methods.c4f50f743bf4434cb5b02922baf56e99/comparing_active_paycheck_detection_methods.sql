-- no_cache

with user_list as(
  select
    *
  from dda2.fct_user_summary
  where ml_user_id not in (select userid from [ml_emp_ids])
  and virtual_account_status = 'Active'
  and enrollment_date >= '2021-01-01'
)

, rm_paychecks_raw as(
  select
    account_id
    , bv_transaction_id
    , description
    , user_id as ml_user_id
    , transaction_date
    , pcd4ca_tagger
    , pcd4ca_isunemploymentbenefit_bol
    , pcd4ca_isgig_bol
    , pcd4ca_istreasurybenefit_bol
  from dda2.fct_bve_rm_paychecks
  where description not ilike '%cashback%' 
  and pcd4ca_tagger NOT IN ('PD|single') 
  and pcd4ca_tagger is not null
  order by transaction_date desc 
)

, rm_paycheck_users as(
  select
    distinct(ml_user_id)
  from rm_paychecks_raw
)

, active_rm_paychecks_users_v1 as(
  select
    distinct(ml_user_id)
  from(
    select
      distinct(ml_user_id)
      , max(transaction_date) as latest_pc_date
    from rm_paychecks_raw
    group by 1
  )
  where [latest_pc_date:day] >= dateadd(day, -35, [current_date:day])
)

, active_rm_paychecks_users as (
  select distinct(ml_user_id)
  from (
    select 
      user_id as ml_user_id
    , pcd4ca_tagger
    , max(transaction_date) as last_rm_pychkon
    , case 
        when pcd4ca_tagger = 'weekly' then 9
        when pcd4ca_tagger = 'bi-weekly' then 17
        when pcd4ca_tagger = 'semi-monthly' then 18
        when pcd4ca_tagger = 'monthly' then 35
        when pcd4ca_tagger = 'PD|weekly' then 9
        when pcd4ca_tagger = 'PD|bi_semi' then 17.5
        when pcd4ca_tagger = 'PD|monthly' then 35
        when pcd4ca_tagger = 'PD|irregular' then 35
        when pcd4ca_tagger = 'PD|single' then 35
      end as tagger_threshold
    , datediff(day, last_rm_pychkon, current_date) diffdays
    , case when diffdays <= tagger_threshold then 'active' else 'inactive' end as incomestatus
    from dda2.fct_bve_rm_paychecks
    where description not ilike '%cashback%' 
      and pcd4ca_tagger NOT IN ('PD|single') 
      and pcd4ca_tagger is not null
    group by 1,2
    )
  where incomestatus = 'active'
  
  union
  
  select distinct
    usr._id as ml_user_id
  from bvenrichment.bank a
  left join bvenrichment.bank_pcd4ca_bank_ddapaycheck c on a.bv_bank_id = c.bv_bank_id
  left join bvenrichment.bank_pcd4ca_bank_allincomes d on a.bv_bank_id = d.bv_bank_id
  LEFT JOIN (select * from lion1.user where brand = 'ml' and deleted <> 1) usr ON a.client_id = usr.clientid
  and c.cluster = d.clusterid
  and date(c.lastpaydate) = date(d.posteddate)
  and d.dda = 'true'
  where a.provider = 'ML'
  and c.isroarmoney = 'true'
  and c.payfrequency is not null
  and nextpaydate >= current_date 
)

, simple_method_only as(
  select a.* from active_rm_paychecks_users_v1 a
  left join active_rm_paychecks_users b
    on a.ml_user_id = b.ml_user_id
  where b.ml_user_id is null
)

, complex_method_only as(
  select a.* from active_rm_paychecks_users a
  left join active_rm_paychecks_users_v1 b
    on a.ml_user_id = b.ml_user_id
  where b.ml_user_id is null
)

, overlapping_users as(
  select a.*
  from active_rm_paychecks_users_v1 a
  inner join active_rm_paychecks_users b
    on a.ml_user_id = b.ml_user_id
--   where b.ml_user_id is not null
)

select
  (select count(distinct ml_user_id) from active_rm_paychecks_users_v1) as simple_method,
  (select count(distinct ml_user_id) from active_rm_paychecks_users) as complex_method,
  (select count(distinct ml_user_id) from simple_method_only) as simple_method_only,
  (select count(distinct ml_user_id) from complex_method_only) as complex_method_only,
  (select count(distinct ml_user_id) from overlapping_users) as overlapping_users