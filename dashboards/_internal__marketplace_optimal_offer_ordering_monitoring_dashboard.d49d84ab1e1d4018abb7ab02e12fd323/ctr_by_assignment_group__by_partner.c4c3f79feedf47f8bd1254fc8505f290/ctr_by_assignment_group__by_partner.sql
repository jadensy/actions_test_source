--by partner
with exp_user as (
select user_id, assignment_group , injection_time
--   min(injection_time) as first_injection_time
from marketplace_static.exp_ooo_model_v0_20220107 as exp
-- group by 1,2 
)


, engagement as (
  select
  	  partner,
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
      --offer_index,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user
  on engagement.userid  = exp_user.user_id 
  and engagement.event_timestamp >= exp_user.injection_time
  where partner!='LoanInATapCard'
  and medium != 'dashboard_web'
  group by 1,2
  order by 1,2,3 desc
)

select *, round(total_click::decimal/total_view,4) as ctr
from engagement
where partner not in ('FutureFuel.io','Be prepared when your job\'s not there')
order by 3 desc