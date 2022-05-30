--monitoring
--by group
--overall
with exp_user as (
select 
  user_id, 
  (case when assignment_group = '-1' then 'control (default Contentful)'
       when assignment_group = '0' then 'control-control (AIDE inject)'
       when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
  injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
  where assignment_group is not null
)

, user_account_age as (
select 
  _id as userid, 
  createdon
from lion1.user
)

select 
assignment_group_name, 
(case when datediff(day, createdon, injection_time) > 30 then '> 30 days' else '< 30 days' end) as account_age_bucket,
count(distinct user_id) as total_users
from exp_user
left join user_account_age 
on exp_user.user_id = user_account_age.userid  
group by 1, 2
order by 2 desc