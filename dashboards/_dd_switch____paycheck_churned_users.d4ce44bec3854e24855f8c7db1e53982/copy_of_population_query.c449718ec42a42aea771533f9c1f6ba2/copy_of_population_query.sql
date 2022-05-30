--no_cache

with user_list as(
  select
    *
  from dda2.fct_user_summary
  --where ml_user_id not in (select userid from [ml_emp_ids])
  --and virtual_account_status = 'Active'
  --and enrollment_date >= '2021-01-01'
)

, rm_paychecks_raw as(
  select 
    user_id as ml_user_id
    , bv_transaction_id
    , pcd4ca_tagger
    , transaction_date
    , pcd4ca_isunemploymentbenefit_bol
    , pcd4ca_isgig_bol
    , pcd4ca_istreasurybenefit_bol,
    lower(description) as low_description,
    REGEXP_replace(low_description, '[0-9]', '') as no_num_desc,
    REGEXP_replace(no_num_desc, ',', '') as no_comma_desc,
    TRIM(no_comma_desc) AS clean_desc, --remove spaces start or end of string only
    SUBSTRING(clean_desc,1,(CHARINDEX(' ',clean_desc + ' ')-1)) as employer
  from dda2.fct_bve_rm_paychecks
  where description not ilike '%cashback%'
    and pcd4ca_tagger NOT IN ('PD|single') 
    and pcd4ca_tagger is not null
  order by transaction_date desc 
)

, rm_paycheck_latest as ( 
  select 
    ml_user_id 
    , employer
    , pcd4ca_tagger
    , max(transaction_date) last_bv_pychkon
  from rm_paychecks_raw 
  group by 1, 2, 3
)

, active_rm_paycheck as(
   select
    *
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
    , datediff(day, last_bv_pychkon, current_date) diffdays
    , case when diffdays <= tagger_threshold then 'active' else 'inactive' end as incomestatus
    from rm_paycheck_latest
)

, rm_paychecks_raw_final as(
    select 
      a.*
      , b.incomestatus
    from rm_paychecks_raw a
    left join active_rm_paycheck b
      on a.ml_user_id = b.ml_user_id 
    and a.employer = b.employer
    and a.pcd4ca_tagger = b.pcd4ca_tagger
)

, rm_paychecks_active as(
    select * from rm_paychecks_raw_final
    where incomestatus = 'active' 
)

--------------------------------------checkpoint-----------------------------------------

, primary_active_pc as (
  select a.client_id,
    c.*,
    d.gig,
    d.isunemploymentbenefit,
    d.istreasurybenefit,
    row_number() over (partition by a.client_id order by lastpaydate desc) as rn
  from bvenrichment.bank a
  left join bvenrichment.bank_pcd4ca_bank_ddapaycheck c on a.bv_bank_id = c.bv_bank_id
  left join bvenrichment.bank_pcd4ca_bank_allincomes d on a.bv_bank_id = d.bv_bank_id
  and c.cluster = d.clusterid
  and date(c.lastpaydate) = date(d.posteddate)
  and d.dda = 'true'

  ---if only need to know which user has active paycheck and summary, can join until here

  --if want to know exact transations related to this income then join this

  -- left join bvenrichment.bank_pcd4ca_bank_primarypaycheckids e on d.bv_bank_id = e.bv_bank_id
  --   left join bvenrichment.base_transaction_enriched f on d.bv_bank_id = f.bv_bank_id and e.primary_paycheck_id = f.bv_transaction_id
  where a.provider = 'ML'
  and c.isroarmoney = 'true'
  and c.payfrequency is not null
  and c.payfrequency NOT IN ('PD|single')
  and nextpaydate >= current_date 
)

, pc_user as (
  select a.client_id, b._id as ml_user_id
  from primary_active_pc a
  inner join lion1.user b
    on a.client_id = b.clientid
)

, active_rm_paychecks_users as(
  select ml_user_id from pc_user
    union
  select distinct ml_user_id from rm_paychecks_active  
)

, rm_paycheck_users as(
  select *
  from dda2.fct_user_summary a
  where ml_user_id in (select distinct ml_user_id from rm_paychecks_raw)  
)

------- checkpoint -----

, user_pc_types as(
  select
    distinct(ml_user_id),
    count(
      case
        when pcd4ca_isgig_bol then bv_transaction_id
      end
    ) as gig_count,
    count(
      case
        when pcd4ca_isunemploymentbenefit_bol then bv_transaction_id
      end
    ) as uib_count,
    count(
      case
        when pcd4ca_istreasurybenefit_bol then bv_transaction_id
      end
    ) as tb_count,
    count(
      case
        when (pcd4ca_isgig_bol is not True and pcd4ca_isunemploymentbenefit_bol is not True and pcd4ca_istreasurybenefit_bol is not True) then bv_transaction_id
      end
    ) as normal_count
  from rm_paychecks_raw
  group by 1
)

, aggre_pc_types as(
  select
    ml_user_id,
    case
      when gig_count > 0 then true else false
    end as has_gig,
    case
      when uib_count > 0 then true else false
    end as  has_uib,
    case
      when tb_count > 0 then true else false
    end as  has_tb,
    case
      when normal_count > 0 then true else false
    end as has_normal
  from user_pc_types
)

, temp_paycheck_status as(
  select
    ml_user_id
    , case
        when(has_gig) then 'GIG'
        when(has_uib) then 'UIB'
        when(has_tb or has_normal) then 'Normal'
        --when (has_gig and not has_uib and not has_tb and not has_normal) then 'GIG Only'
        --when (not has_gig and has_uib and not has_tb and not has_normal) then 'UIB Only'
        --when (not has_gig and not has_uib and has_tb and not has_normal) then 'TB Only'
        --when (not has_gig and not has_uib and not has_tb and has_normal) then 'Normal Only'
        --else 'Multiple Paychecks'
      end paycheck_status
  from aggre_pc_types
)

, join_temp as (
  select
    a.ml_user_id
    , a.enrollment_date
    , case
        when b.ml_user_id is not null then True else False
      end has_rm_paycheck
    , c.paycheck_status
    , case
        when d.ml_user_id is not null then True else False
      end has_active_paycheck
  from user_list a
  left join rm_paycheck_users b
    on a.ml_user_id = b.ml_user_id
  left join temp_paycheck_status c
    on a.ml_user_id = c.ml_user_id
  left join active_rm_paychecks_users d
    on a.ml_user_id = d.ml_user_id
)

, aggre as(
  select
    *
  from join_temp
  where has_rm_paycheck
  and not has_active_paycheck
)

select
  distinct(paycheck_status) as paycheck_type
  , count(distinct ml_user_id) as total_users
from aggre
group by 1