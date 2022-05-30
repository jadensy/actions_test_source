-- no_cache

with active_users as(
  select
    *
  from dda2.fct_user_summary
  where virtual_account_status = 'Active'
  and ml_user_id not in (select userid from [ml_emp_ids])
  and enrollment_date >= '2021-01-01'
)

, rm_paychecks as(
  select
    *
  from dda2.fct_bve_rm_paychecks
  where description not ilike '%cashback%' 
  and pcd4ca_tagger NOT IN ('PD|single') 
  and pcd4ca_tagger is not null
)

, users_with_rm_paychecks as(
  select
    a.*
  from rm_paychecks a
  left join active_users b
    on a.user_id = b.ml_user_id
  where b.ml_user_id is not null
)

select
  distinct ml_user_id
from(
  select
    distinct(user_id) as ml_user_id
    , max(transaction_date) as last_paycheck
  from users_with_rm_paychecks
  group by 1
)
where [last_paycheck:day] < [getdate():day] - interval '45 days'