-- This dashboard/chart has been updated by DE to apply the DDA2 schema migration changes on 2020-11-01 03:18:20.170922 
-- For more info, see https://moneylion.atlassian.net/wiki/spaces/DE/pages/1809711844/DDA2+Schema+Migration+NOV+2020

with overall as (
  select *
  from [pf_mkt_attr_users_jump450]
),

--To find out at the end, among the users who have cb membership, who has a taken/funded a loan
cb_loan_taken as (
  select
    a.userid,
    b.email,
    min(date(approveddate)) as first_date,
    min(approveddate) as first_timestamp
  from ml_finance.fpall_ll a 
  join lion1.user b on a.userid = _id
  where isfunded = 1 
    and b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
  group by 1,2
  having min(date(approveddate)) is not null
),

--To find out among those who have IC SU, and has taken 1st instacash
user_first_ic1 as(
  select 
    a.user_id as userid, 
    min(date(a.created_at)) as first_date,
    min(a.created_at) as first_timestamp
  from cashadvance_marketing.fct_first_ic_taken a
  join ml_public.addonhistory b on a.user_id = b.userid and date(a.created_at) >= date(b.createdon)
  where true
  group by 1
  having min(date(a.created_at)) is not null
),

user_first_ic as(
  select 
    a.userid, 
    b.email,
    a.first_date,
    a.first_timestamp
  from user_first_ic1 a
  join lion1.user b on a.userid = b._id
  where true
    and b.email is not null
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
),

-- Funded Roar Money Accounts

rm_earliest_deposit_dates as (
  select distinct
    a.ml_user_id as userid,
    b.email as email,
    min(date(a.transaction_created)) as first_date,
    min(a.transaction_created) as first_timestamp
  from dda2_raw.transaction a
  left join lion1.user b on a.ml_user_id = b._id
  where true
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
    and a.type + a.o_type in (
     select trans_type from (
        select distinct 
          *, 
          type + o_type as trans_type
        from (
          select 
            type, 
            o_type 
          from dda2_raw.transaction
          where true
            and type = 'pmt' --AND o_type != 'IP'

          union all 

          select 
            type, 
            o_type
          from dda2_raw.transaction
          where true
            and type = 'adj' 
            and o_type = 'NC'
        )
      )
    )
    and a.amount > 0 
  group by 1,2
),

total_cost as (
  select
    *,
    [date_stop:month] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
),


/**************************************************************************************************
**  Getting information for Web Campaign which goes through the following logic:
**  Unbounce to get page path with campaign id in the utm links to capture uers from web logic 
**  +
**  AppsFlyer - get users who have SU with campaign names that have 'webflow' (will be in next session)
**************************************************************************************************/

--Gets campaign and user information from unbounce
landing_page_agg as (
  select 
    case when page_path like '/google%' then 'google'
         when page_path like '/facebook%' then 'facebook'
         when page_path like '/bing%' then 'bing'
         else page_path end as ad_platform,
    unbounce."timestamp",
    unbounce.anonymous_id
  from unbounce.page_loaded as unbounce
  
  where true 
    and url_hostname = 'get.moneylion.com'
    and (unbounce.page_path like '/bing%' or unbounce.page_path like '/google%' or unbounce.page_path like '/facebook%')
  
  union all
  
  select
    case when lower(homepage_web.context_campaign_source) like '%google%' then 'google'
         when lower(homepage_web.context_campaign_source) like '%facebook%' then 'facebook'
         when lower(homepage_web.context_campaign_source) like '%bing%' then 'bing'
         else lower(homepage_web.context_campaign_source) end as ad_platform,
    homepage_web."timestamp",
    homepage_web.anonymous_id
  from homepage_web.page_loaded as homepage_web
  where true
    and homepage_web.url_hostname = 'www.moneylion.com'
    and homepage_web.context_campaign_source is not null
    and homepage_web.context_campaign_medium = 'cpc'
  
)

select distinct
  coalesce(cost.week_start_date,[c.touchtime:month]) as week_start_date,
  coalesce(cost.ad_platform, c.ad_platform) as ad_platform,
  cast(cost.total_cost as decimal(16,2)) as "Cost",
  avg(cost.total_impressions) as "Ad Impressions",
  avg(cost.total_clicks) as "Ad Clicks",
  cast(avg(cost.total_clicks)*1.0/avg(cost.total_impressions) as decimal(16,4)) as "Ad CTR %",  
  cast(count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end)*1.0/avg(cost.total_clicks) as decimal(16,4))as "New SU CVR %",
  avg(cost.total_cost)*1000.0/nullif(avg(cost.total_impressions),0) as "CPM $",
  avg(cost.total_cost)*1.0/nullif(avg(cost.total_clicks),0) as "CPC $",
  count(distinct case when c.touchtime >= '2020-03-13' and c.product = 'new core' then c.email end) as "New Core SU",
  count(distinct case when c.product = 'roar money' then c.email end) as "RM SU",
  count(distinct case when c.product = 'cbplus' then c.email end) as "CB+ SU",
  count(distinct case when c.product = 'instacash' then c.email end) as "IC SU",
  count(distinct case when c.product = 'investment' then c.email end) as "Wealth SU",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end) as "Total New SU #",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end),0) as decimal(16,2)) as "Paid CAC $",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end)*1.0/nullif(count(distinct case when c.touchtime >= '2020-03-13' and c.product = 'new core' then c.email end),0) as "Core to SU %",

  count(distinct case when c.product = 'roar money' then fundedrm.email end) as "RM Funded",
  cast(count(distinct case when c.product = 'roar money' then fundedrm.email end)*1.0/nullif(count(distinct case when c.product = 'roar money' then c.email end),0) as decimal(16,4)) as "RM Funded %",
  count(distinct case when c.product = 'cbplus' then cbloan.email end) as "CB+ Funded",
  cast(count(distinct case when c.product = 'cbplus' then cbloan.email end)*1.0/nullif(count(distinct case when c.product = 'cbplus' then c.email end),0) as decimal(16,4)) as "CB+ Funded %",
  count(distinct case when c.product = 'instacash' then takeic.email end) as "Ic Taken",
  cast(count(distinct case when c.product = 'instacash' then takeic.email end)*1.0/nullif(count(distinct case when c.product = 'instacash' then c.email end),0) as decimal(16,4)) as "IC Taken %",
 
  avg(lp_visits.page_visits) as "Page Visits",
  cast(count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end) as decimal(16,2))/nullif(avg(lp_visits.page_visits),0) as "CVR From Page Visits %",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('instacash') then c.email end),0) as decimal(16,2)) as "IC Paid CAC $",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('cbplus') then c.email end),0) as decimal(16,2)) as "CB CAC $",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('roar money') then c.email end),0) as decimal(16,2)) as "RM Paid CAC $",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('investment standalone') then c.email end),0) as decimal(16,2)) as "Wealth Paid CAC $"

from (
  select 
    [date_stop:month] as week_start_date,
    ad_platform,
    sum(total_cost) as total_cost,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from total_cost
  where (lower(name) like '%webflow%' or lower(name) like '%homepage%')
  group by 1,2
  having sum(total_cost) > 0
  ) cost
full outer join (
  select * 
  from overall 
  where true
    and source = 'web campaign'
)c 
  on cost.week_start_date = [c.touchtime:month] and c.ad_platform = cost.ad_platform
left join (
  select 
    [date("timestamp"):month] as week_start_date,
    ad_platform,
    count(distinct anonymous_id) as page_visits
  from landing_page_agg
  group by 1,2
)lp_visits 
  on lp_visits.week_start_date = [c.touchtime:month] and lp_visits.ad_platform = c.ad_platform
left join cb_loan_taken cbloan 
  on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
left join user_first_ic takeic 
  on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
left join rm_earliest_deposit_dates fundedrm 
  on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime
where true
  and coalesce(cost.ad_platform, c.ad_platform) = 'facebook'
  and coalesce(cost.week_start_date,[c.touchtime:month]) >= date('2020-01-01')
group by 1,2,3
order by 1 desc