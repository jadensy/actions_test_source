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

, rm_paycheck_users as(
  select
    distinct(ml_user_id)
  from rm_paychecks_raw
)

, active_rm_paychecks_users as (
    select ml_user_id
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
    
    select
      distinct(usr._id) as ml_user_id
    from bvenrichment.bank a
    left join bvenrichment.bank_pcd4ca_bank_ddapaycheck c
      on a.bv_bank_id = c.bv_bank_id
    left join bvenrichment.bank_pcd4ca_bank_allincomes d
      on a.bv_bank_id = d.bv_bank_id
    LEFT JOIN (select * from lion1.user where brand = 'ml' and deleted <> 1) usr
      ON a.client_id = usr.clientid
      and c.cluster = d.clusterid
      and date(c.lastpaydate) = date(d.posteddate)
      and d.dda = 'true'
    where a.provider = 'ML'
    and c.isroarmoney = 'true'
    and c.payfrequency is not null
    and nextpaydate >= current_date 
)

---------------------------------- checkpoint -------------------------------------

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
        when (has_gig and not has_uib and not has_tb and not has_normal) then 'GIG Only'
        when (not has_gig and has_uib and not has_tb and not has_normal) then 'UIB Only'
        when (not has_gig and not has_uib and has_tb and not has_normal) then 'TB Only'
        when (not has_gig and not has_uib and not has_tb and has_normal) then 'Normal Only'
        else 'Multiple Paychecks'
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

-------------------------- checkpoint -----------------------------------

select
  distinct(paycheck_status) as paycheck_type
  , count(distinct ml_user_id) as total_users
from aggre
group by 1