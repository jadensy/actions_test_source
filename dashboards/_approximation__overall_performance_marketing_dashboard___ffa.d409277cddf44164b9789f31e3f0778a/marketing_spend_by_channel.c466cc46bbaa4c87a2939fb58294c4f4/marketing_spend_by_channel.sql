/**************************************************************************************************
**  Combining both App Campaign and Web Campaign and ranking them
**  For App Campaigns just filter for non webflow campaigns from AppsFlyer
**  For Web Campaigns we need to add unbounce data with AppsFyer webflow campaign data
**************************************************************************************************/

with total_cost as (
  select
    *,
    [date_stop:week] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
)

select 
 [date_stop:week] as week_start_date,
  ad_platform,
  sum(total_cost) as total_cost
from total_cost
-- where lower(name) not like '%webflow%'
where week_start_date >= date('2020-01-01')
group by 1,2