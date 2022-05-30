--no_cache
with overall as (
select 
paid_ffa_last_ad_click_dtime as touchtime,
paid_ffa_type as product,
email,
userid,
paid_ffa_channel as ad_platform,
paid_ffa_campaign_name as campaign,
paid_ffa_campaign_id as campaign_id,
paid_ffa_su_dtime as event_first_timestamp,
paid_ffa_campaign_type as source

from marketing.fct_pfmkt_user_view_ffa 
where 1=1
and paid_ffa_type is not null)

, IC_denial as (
 select email, onboarding_screen, min(first_IC_denial_time) as first_IC_denial_time,
  min(first_IC_screen_date) as first_IC_screen_date from 
(
select user_id as email, min(timestamp) as first_IC_denial_time,
  min(date(timestamp)) as first_IC_screen_date, 'IC denial' as onboarding_screen
from android.onboarding_checkpoint_hit
where true
and name = 'Instacash To RM CrossSell'
group by 1

union all

select user_id as email, min(timestamp) as first_IC_denial_time,
  min(date(timestamp)) as first_IC_screen_date, 'IC denial' as onboarding_screen
from ios.onboarding_checkpoint_hit
where true
and name = 'Instacash To RM CrossSell'
group by 1)
group by 1,2)

, temp1 as (
select a.*, b.first_IC_screen_date, b.first_IC_denial_time
from overall a 
left join IC_denial b 
on a.email = b.email and a.event_first_timestamp > b.first_IC_denial_time
where a.source = 'app campaign'
)

select [touchtime:month] as reporting_month,
ad_platform,
source,
campaign,
count(distinct userid) as user_count,
count(distinct case when product = 'instacash' then userid end) as IC_FFA,
count(distinct case when product = 'roar money' then userid end) as RM_FFA,
count(distinct case when product = 'roar money' and first_IC_screen_date is not null then userid end) as "RM_FFA (IC Denial)",
count(distinct case when product = 'cbplus' then userid end) as CB_FFA,
count(distinct case when product = 'investment standalone' then userid end) as Inv_FFA
from temp1
where reporting_month >= '2020-07-01'
and source = 'app campaign'
group by 1,2,3,4
order by 1 desc, 2,3,4