--monitoring
--by group
--overall
with exp_user as (
select 
  user_id, 
  assignment_group, 
  injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
)

, user_account_age as (
select 
  _id as userid, 
  createdon
from lion1.user
)

, final_table as (
select exp_user.*, 
  (case when datediff(day, createdon, injection_time) > 30 then '> 30 days' else '< 30 days' end) as account_age_bucket
from exp_user
left join user_account_age 
on exp_user.user_id = user_account_age.userid  
)  
  
  
select 
  account_age_bucket,
  count(distinct user_id) as total_users  
from final_table
where account_age_bucket is not null
group by 1
order by 1