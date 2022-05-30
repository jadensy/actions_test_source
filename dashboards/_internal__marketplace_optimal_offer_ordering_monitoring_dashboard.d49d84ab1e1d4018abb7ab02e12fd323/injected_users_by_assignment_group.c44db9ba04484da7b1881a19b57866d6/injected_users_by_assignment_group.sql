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

select assignment_group_name, count(distinct user_id) as total_users
from exp_user
group by 1
order by 2 desc