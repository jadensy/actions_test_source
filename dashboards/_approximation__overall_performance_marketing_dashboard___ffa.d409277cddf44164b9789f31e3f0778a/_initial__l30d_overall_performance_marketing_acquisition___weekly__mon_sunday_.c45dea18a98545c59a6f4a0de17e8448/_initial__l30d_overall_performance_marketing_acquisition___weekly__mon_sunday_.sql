-- This dashboard/chart has been updated by DE to apply the DDA2 schema migration changes on 2020-11-01 03:18:20.170922 
-- For more info, see https://moneylion.atlassian.net/wiki/spaces/DE/pages/1809711844/DDA2+Schema+Migration+NOV+2020

--Membership users--
with icwb_user as(
  select
    userid, 
    min(date(createdon)) as first_date,
    min(createdon) as first_timestamp,
    'instacash' as product
  from ml_public.addonhistory
  group by 1
  having min(date(createdon)) is not null
),

--get the date user created cbplus membership
cbplus_user as(
  select
    userid,
    min(date(subscriptionconfigurationidhistories_createdon)) as first_date,
    min(subscriptionconfigurationidhistories_createdon) as first_timestamp,
    'cbplus' as product
  from ml_public.subscription_configuration
  where true
    and subscriptionconfigurationidhistories_subscriptionconfigurationid = 'M03'
    and subscriptionconfigurationidhistories_status = 'active'
  group by 1
  having min(date(subscriptionconfigurationidhistories_createdon)) is not null
),

roarmoney_user as (
  select 
    a.ml_user_id as userid, 
    min(date(a.account_created)) as first_date,
    min(a.account_created) as first_timestamp,
    'roar money' as product
  from dda2_raw.account a 
  left join lion1.user b on a.ml_user_id = b._id
  where true
    and a.account_type = 'Virtual'
--     and a.ml_user_id not in (select distinct userid from [ml_emp_ids])
    and b.email not like '%metabank%'
    and date(account_created) >= DATE('2020-05-26')
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
  group by 1
),

--all user who at least one point have a dda acount
dda_1_user as(
  select 
--     email_address as email,
    b._id as userid,
    min(date(created_at)) as first_date,
    min(created_at) as first_timestamp,
    'dda' as product
  from ml_public.postgres_customers a
  left join lion1.user b on a.email_address = b.email
  where true
--     and status = 'VERIFIED'
    and b.email not like '%moneylion%'
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null)
  group by 1
  having min(date(created_at)) is not null
),

ml_plus_users as (
  select 
    a.userid,
    min(date(a.subscriptionconfigurationidhistories_createdon)) as first_date,
    min(a.subscriptionconfigurationidhistories_createdon) as first_timestamp,
    'ML-Plus' as product
  from ml_public.subscription_configuration a
--   left join lion1.user b ON a.userid = b._id
  where true
    and a.subscriptionconfigurationidhistories_type = 'ML-Plus'
    and a.subscriptionconfigurationidhistories_status != 'pending'
    and a.userid not in (select userid from [ml_emp_ids])
--   and b.email is not null
  group by 1
),

investment_int_type as (
  select a.userid, 
    min(date(c.createdon)) as first_date,
    min(c.createdon) as first_timestamp,
    'investment' as product
  from ml_public.drivewealthuser as a
  left join ml_public.drivewealth_accountuser as b
    on a._id = b.drivewealthuserid
  left join ml_public.drivewealth_account as c
    on b.drivewealthaccountid = c._id
  where true
    and c.accountstatus not in  ('PENDING','CLOSED')
    and c.internaltype = 'INVESTMENT'
    and a.userid not in (select distinct userid from [ml_emp_ids])
  group by 1
  having min(c.createdon) >= '2020-03-16'
),

escrow_int_type as (
  select a.userid, 
    min(date(c.createdon)) as first_date,
    min(c.createdon) as first_timestamp,
    'escrow' as product
  from ml_public.drivewealthuser as a
  left join ml_public.drivewealth_accountuser as b
    on a._id = b.drivewealthuserid
  left join ml_public.drivewealth_account as c
    on b.drivewealthaccountid = c._id
  where true
    and c.accountstatus not in  ('PENDING','CLOSED')
    and c.internaltype = 'ESCROW'
    and a.userid not in (select distinct userid from [ml_emp_ids])
  group by 1
  having min(c.createdon) >= '2020-03-16'
),

standalone_investment_user as (
  select a.*
  from investment_int_type a 
  left join escrow_int_type b 
    on a.userid = b.userid 
  where true
    and a.first_date < b.first_date
    or b.first_date is null
),

inv_user_email as(
  select distinct *
  from (
    select a.*, b.email
    from standalone_investment_user a
    left join lion1.user b on a.userid = b._id
    where true
      and b.brand = 'ml' 
      and (b.deleted <> 1 or b.deleted is null)
  )
  where email is not null
),

CB_screen_viewed as (
  select user_id, "timestamp" as dtime, context_os_name as os_type, screen
  from ios.screen_viewed
  where user_id is not null and screen = 'CBPlusCreditReserveIntro'

  union all

  select user_id, "timestamp" as dtime, context_os_name as os_type, screen
  from prod.screen_viewed
  where user_id is not null and screen = 'CBPlusCreditReserveIntro'

  union all

  select user_id, "timestamp" as dtime, context_os_name as os_type, screen
  from android.screen_viewed
  where user_id is not null and screen = 'CBPlusCreditReserveIntro'
),

CB_screen_users as (
  select user_id, min(date(dtime)) as CB_screen_date
  from CB_screen_viewed
  group by 1
),

SNB_inv_event_fired as (
  select user_id, "timestamp" as dtime, context_device_type as os_type, referrar as origin, navigate_from
  from ios.wealth_account_creation_initiated
  where user_id is not null and referrar = 'Shake ''N'' Bank'
  
  union all
  
  select user_id, "timestamp" as dtime, context_device_type as os_type, referrar as origin, navigate_from
  from android.wealth_account_creation_initiated
  where user_id is not null and referrar = 'Shake ''N'' Bank'
),

SNB_event_users as (
  select user_id, min(date(dtime)) as SNB_event_date
  from SNB_inv_event_fired
  group by 1
),

inv_users_not_from_CB as (
  select a.*, b.CB_screen_date 
  from inv_user_email a 
  left join CB_screen_users b 
    on a.email = b.user_id 
  where a.first_date < b.CB_screen_date 
    or b.CB_screen_date is null
),

final_inv_standalone_users as ( -- users not from CB+, SNB 
  select a.*, b.SNB_event_date 
  from inv_users_not_from_CB a 
  left join SNB_event_users b 
    on a.email = b.user_id 
  where a.first_date < b.SNB_event_date 
    or b.SNB_event_date is null
),

new_core_union as (
	select 
		userid,
		min(date(a.coreidv_dateverified)) as first_date,
		min(a.coreidv_dateverified) as first_timestamp
  	from membership.userverification_coreidv a
	where true
		and a.coreidv_checkresultresult = 'passed'
  group by 1
		
	union all
	
	select
		user_id as userid,
		min(date(created_on)) as first_date,
		min(created_on) as first_timestamp
	from membership.verification
	where true
		and transaction_status = 'passed'
  group by 1
),

new_core_user as (
  select
    b.email as email,
    min(date(a.first_timestamp)) as first_date,
    min(a.first_timestamp) as first_timestamp,
    'new core' as product
  from new_core_union a
  join lion1.user b on a.userid = b._id
  where true
    and a.userid not in (select userid from [ml_emp_ids])
    and b.brand = 'ml' 
    and (b.deleted <> 1 or b.deleted is null) 
--     and b.email is not null
  group by 1
),

-- Only register a user's first IC/CB membership acquired - to remove the duplicates so that IC + CB count = Paid Membership exactly
first_paid_membership_2 as (
  select 
    userid, 
    first_date, 
    first_timestamp, 
    product, 
    row_number() over (partition by userid order by first_timestamp asc) as rn 
  from (
    select * from icwb_user
      union all
    select * from cbplus_user
      union all
    select * from roarmoney_user
      union all
    select * from dda_1_user
      union all
    select * from ml_plus_users
      union all
    select userid, first_date, first_timestamp, product from final_inv_standalone_users
  )
  where true
    and userid not in (select userid from [ml_emp_ids])
),

--for instacash and cb tables earlier, we need to get the emails through this method
--part 1
combine_ic_cb_user as(
  select 
    userid, 
    first_date, 
    first_timestamp, 
    product 
  from first_paid_membership_2 
  where true
    and rn = 1
),

--part 2
combine_user_email as(
  select *
  from (
    select a.*, b.email as email
    from combine_ic_cb_user a
--     left join lion1.user b on a.userid = _id
    left join lion1.user b on a.userid = b._id
    where true
      and b.brand = 'ml' 
      and (b.deleted <> 1 or b.deleted is null)
  )
  where true
    and email is not null
),

--part 3: Combine CB/IC members with Core members
combine_all as(
  select 
    *,
    [first_date:week] as week_start_date
  from (
    select email, first_date, first_timestamp, product from combine_user_email
    union all
    select * from new_core_user
  )
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

/**************************************************************************************************
**  Getting information for Web Campaign which goes through the following logic:
**  Unbounce to get page path with campaign id in the utm links to capture uers from web logic 
**  +
**  AppsFlyer - get users who have SU with campaign names that have 'webflow' (will be in next session)
**************************************************************************************************/

--Gets campaign and user information from unbounce
landing_page_agg as (
  select 
    unbounce.page_path,
    unbounce."timestamp",
    split_part(replace(replace(replace(replace(replace(split_part(context_page_search, '&', 3), '%7C', '|'),'%257C','|'),'%C2%BF','_'),'%2F', '/'),'%252F','/'),'=',2) utm_campaign,
    context_page_url,
    context_page_search,
    unbounce.anonymous_id,
    coalesce(prod_and_onb.user_id,unbounce.user_id) as email,
    dense_rank() over (partition by coalesce(prod_and_onb.user_id,unbounce.user_id) order by unbounce."timestamp" desc) as rn
  from unbounce.page_loaded as unbounce
  left join (
    select 
      anonymous_id,
      user_id
    from prod.page_loaded
    where true
      and user_id is not null
    
    union all
    
    select
      anonymous_id,
      user_id
    from onboarding_web.page_loaded
    where user_id is not null
  ) prod_and_onb on prod_and_onb.anonymous_id = unbounce.anonymous_id
  
  where true 
    and url_hostname = 'get.moneylion.com'
    and (unbounce.page_path like '/bing%' or unbounce.page_path like '/google%' or unbounce.page_path like '/facebook%')
    and date(unbounce."timestamp") >= current_date - interval '30 days'
),

-- Join unbounce data with IC & CB & Core Users
web_campaign_breakdown as (
  select
    c.email,
    week_start_date as event_week_start_date,
    c.first_timestamp as event_first_timestamp,    
    product,
    lp.rn,
    lp.page_path,
    lp.utm_campaign,
    lp."timestamp" as lp_touchtime
  from landing_page_agg lp
  left join combine_all c on lp.email = c.email 
    and c.first_timestamp between lp."timestamp" and lp."timestamp" + interval '30 days'
  where true 
--     and c.week_start_date >= current_date - interval '30 days'
    and utm_campaign is not null
    and utm_campaign <> ''
),

--Gets the finalized web (unbounce only) last touch for conversion
web_campaign as (
  select
    w.email,
    w.event_week_start_date,
    w.event_first_timestamp,
    w.product,
    w.page_path,
    w.lp_touchtime,
    w.utm_campaign,
    w.rn,
    [w.lp_touchtime:week] as week_click_date  

  from web_campaign_breakdown w
  inner join (
    select
      w.email,
      w.product,
      min(w.rn) as last_touch
    from web_campaign_breakdown w
    group by 1,2
  ) p on p.email = w.email and p.product = w.product and p.last_touch = w.rn
  where true 
--     and date(w.lp_touchtime) >= date('2020-01-01')
  
),

/**************************************************************************************************
**  Getting information for App Campaign (and also some 'webflow') which goes through the following logic:
**  AppsFlyer Api will attribute the users who have SU from campaign - which we can get email
**  And map the email back to our membership users
**************************************************************************************************/

-- get latest email for all appsflyer id
appsflyer_complete_email as (
  select
    appsflyer_id,
    customer_user_id
  from(
    select distinct
      appsflyer_id,
      customer_user_id,
      row_number() over (partition by appsflyer_id order by TO_TIMESTAMP(event_time, 'YYYY-MM-DD HH24:MI:SS') desc) as rn
    from appsflyerapi.users
    where true
      and customer_user_id is not null
      and customer_user_id <> 'null'
   ) a 
  where true
    and rn = 1
),

-- find users that and their attributed install campaign
events as(
  select
    install_time as start_timestamp,
    installations.media_source,
    case when lower(installations.media_source) like '%google%' then 'google'
         when lower(installations.media_source) like '%facebook%' then 'facebook'
         when lower(installations.media_source) like '%bing%' then 'bing'
         when lower(installations.media_source) like '%snapchat%' then 'snapchat'
         when lower(installations.media_source) like '%apple%' then 'apple'
         when lower(installations.media_source) like '%restricted%' then 'facebook'
         else lower(installations.media_source) end as ad_platform,
    installations.campaign,
    installations.appsflyer_id as installs,  
    installations.campaign_id,
    coalesce(installations.customer_user_id,  appsflyer_complete_email.customer_user_id) as customer_user_id

  from (  -- Installations
    select
      install_time::timestamp as install_time,
      event_time::timestamp as event_time,
      media_source,
      campaign,
      campaign_id,
      appsflyer_id,
      customer_user_id
    from appsflyerapi.users 
    where true
      and event_name in ('install', 'Install Attributed') --When including install attributed it does not match the numbers seen in installs from appsflyer website
      and media_source in ('Facebook Ads', 'googleadwords_int', 'Apple Search Ads', 'snapchat_int', 'restricted')
      and install_time >= current_date - interval '30 days'
  ) installations
--  Have omitted the joins to get appsflyer core, ic, and cb linkage. Only get the 'installation' attributed to appsflyer or 'reattribution/retargeting'
 
  left join appsflyer_complete_email on appsflyer_complete_email.appsflyer_id = installations.appsflyer_id
  where true
--     and install_time >= date('2020-01-01')
    and installations.media_source is not null
),
  
-- join the above users with our membership users
app_campaign as (
  select
    date(start_timestamp) as install_date,
    [install_date:week] as week_start_date,  
    start_timestamp as install_timestamp,
    media_source,
    ad_platform,
    campaign,
    campaign_id,
    c.product,
    c.email,
    c.week_start_date as event_week_start_date,
    c.first_timestamp as event_first_timestamp

  from events
  left join combine_all c on events.customer_user_id = c.email 
    and c.first_timestamp between events.start_timestamp and events.start_timestamp + interval '30 days'
  where true 
--     and date(start_timestamp) >= date('2020-01-01')
),

-- Same as above but for retargeting section users
appsflyer_complete_email_retargeting as (
  select
    appsflyer_id,
    customer_user_id
  from(
    select
      appsflyer_id,
      customer_user_id,
      row_number() over (partition by appsflyer_id order by TO_TIMESTAMP(event_time, 'YYYY-MM-DD HH24:MI:SS') desc) as rn
    from appsflyerapi.users_retargeting
    where true
      and customer_user_id is not null
      and customer_user_id <> 'null'
   ) a 
  where true
    and rn = 1
),

events_retargeting as(
  select
    install_time as start_timestamp,  
    installations.media_source,
    case when lower(installations.media_source) like '%google%' then 'google'
         when lower(installations.media_source) like '%facebook%' then 'facebook'
         when lower(installations.media_source) like '%bing%' then 'bing'
         when lower(installations.media_source) like '%snapchat%' then 'snapchat'
         when lower(installations.media_source) like '%apple%' then 'apple'
         when lower(installations.media_source) like '%restricted%' then 'facebook'
         else lower(installations.media_source) end as ad_platform,
    installations.campaign,
    installations.campaign_id,
    installations.appsflyer_id as installs,  
    coalesce(installations.customer_user_id,  appsflyer_complete_email_retargeting.customer_user_id) as customer_user_id

  from ( -- Retargeting
    select
      install_time::timestamp as install_time,
      event_time::timestamp as event_time,
      media_source,
      campaign,
      campaign_id,
      appsflyer_id,
      customer_user_id
    from appsflyerapi.users_retargeting 
    where true
      and event_name in ('re-engagement', 're-attribution') --When including install attributed it does not match the numbers seen in installs from appsflyer website
      and media_source in ('Facebook Ads', 'googleadwords_int', 'Apple Search Ads', 'snapchat_int', 'restricted')
      and install_time >= current_date - interval '30 days'
  ) installations
--  Have omitted the joins to get appsflyer core, ic, and cb linkage. Only get the 'installation' attributed to appsflyer or 'reattribution/retargeting'
  left join appsflyer_complete_email_retargeting on appsflyer_complete_email_retargeting.appsflyer_id = installations.appsflyer_id
  where true 
--     and install_time >= date('2020-01-01')
    and installations.media_source is not null
),

app_campaign_retargeting as (
  select
    date(start_timestamp) as retarget_date,
    [retarget_date:week] as week_start_date,
    start_timestamp as retarget_timestamp,
    media_source,
    ad_platform,
    campaign,
    campaign_id,
    c.product,
    c.email,
    c.week_start_date as event_week_start_date,
    c.first_timestamp as event_first_timestamp
  from events_retargeting
  left join combine_all c on events_retargeting.customer_user_id = c.email 
    and c.first_timestamp between events_retargeting.start_timestamp and events_retargeting.start_timestamp + interval '30 days'
  where true 
--     and date(start_timestamp) >= date('2020-01-01')
),

/**************************************************************************************************
**  Combining both App Campaign and Web Campaign and ranking them
**  For App Campaigns just filter for non webflow campaigns from AppsFlyer
**  For Web Campaigns we need to add unbounce data with AppsFyer webflow campaign data
**************************************************************************************************/

total_cost as (
  select
    *,
    [date_stop:week] as week_start_date
  from (
    --Facebook
    select
      date(date_stop) as date_stop,
      campaign_names.name,
      ads_data.campaign_id,
      'facebook' as ad_platform,
      sum(spend) as total_cost,
      sum(link_clicks) as total_clicks,
      sum(impressions) as total_impressions
    from (
      select distinct 
        ad_id, 
        date_stop, 
        spend, 
        link_clicks, 
        impressions
      from facebookads.insights
      where true
        and date_stop >= current_date - interval '30 days'
    ) cost_data
    inner join (
      select distinct 
        id, 
        campaign_id
      from facebookads.ads
    ) ads_data on cost_data.ad_id = ads_data.id
    inner join (
      select distinct 
        id, 
        name, 
        split_part(name, '_|_', 8) as product
      from facebookads.campaigns
    ) campaign_names on ads_data.campaign_id = campaign_names.id
    group by 1,2,3,4

    union all

    --Google Ads
    select
      date(date_stop) as date_stop,
      campaign_names.name,  
      cost_data.campaign_id,
      'google' as ad_platform,    
      sum(cost) / 1000000.0 as total_cost,
      sum(clicks) as total_clicks,
      sum(impressions) as total_impressions
    from(
      select 
        campaign_id, 
        date_stop, 
        received_at, 
        cost, clicks, 
        impressions, 
        row_number() OVER (PARTITION by campaign_id, date_stop ORDER BY received_at DESC) as rn 
      from adwords.campaign_performance_reports
      where true
        and date_stop >= current_date - interval '30 days'
    ) cost_data
    inner join(
      select distinct 
        id, 
        name, 
        split_part(name, '_|_', 8) as product
      from adwords.campaigns 
    ) campaign_names on cost_data.campaign_id = campaign_names.id
    where true
      and rn = 1
    group by 1,2,3,4

    union all

    --Bing Ads
    select 
      date(timeperiod) as date_stop,
      campaignname as name,
      cast(campaignid as varchar) as campaign_id,
      'bing' as ad_platform,
      sum(spend) as total_cost,
      sum(clicks) as total_clicks,
      sum(impressions) as total_impressions
    from (
      select distinct 
        timeperiod,
        campaignname,
        campaignid,
        spend,
        clicks,
        impressions
      from bingads.campaign_performance
      )
    where true
      and date(timeperiod) >= current_date - interval '30 days'
    group by 1,2,3,4

    union all

    select 
      date("date") as date_stop,
      metadata_campaignname as name,
      cast(metadata_campaignid as varchar) as campaign_id,
      'apple' as ad_platform,
      sum(localspend_amount) as total_cost,
      sum(taps) as total_clicks,
      sum(impressions) as total_impressions
    from ds_growth.applesearchads_reports_campaign
    where true 
      and "date" >= current_date - interval '30 days'
    group by 1,2,3,4

    union all

    select distinct 
      start_date as date_stop,
      c.name,
      s.id as campaign_id,
      'snapchat' as ad_platform,
      spend_usd as total_cost,
      swipes as total_clicks,
      impressions as total_impressions
    from snapchat.campaign_stat s
    left join snapchat.campaign c on c.id = s.id
    where true 
      and date(s.start_time) >= current_date - interval '30 days'
  )
),

app_campaign_merged as (
  select
    week_start_date,
    install_timestamp as touchtime,
    media_source,
    ad_platform,
    campaign,
    campaign_id,
    product,
    email,
    event_week_start_date,
    event_first_timestamp
  from app_campaign
  where true 
    and email is not null  
  
  union all
  
  select
    week_start_date,
    retarget_timestamp as touchtime,
    media_source,
    ad_platform,
    campaign,
    campaign_id,
    product,
    email,
    event_week_start_date,
    event_first_timestamp
  from app_campaign_retargeting
  where true
    and email is not null  
),

overall as (
  --info from unbounce LP
  select *, row_number() over (partition by email, product order by touchtime desc) as last_touch_for_prod
  from (
    select
      week_start_date,
      touchtime,
      ad_platform,
      campaign,
      campaign_id,
      product,
      email,
      event_week_start_date,
      event_first_timestamp,
      'web campaign' as source
    from (
      select 
        week_click_date as week_start_date,
        lp_touchtime as touchtime,
        case when page_path like '/google%' then 'google'
             when page_path like '/facebook%' then 'facebook'
             when page_path like '/bing%' then 'bing'
             else page_path end as ad_platform,
        campaign_name.name as campaign,
        web_campaign.utm_campaign as campaign_id,
        product,
        email,
        event_week_start_date,
        event_first_timestamp
      from web_campaign
      left join (
        select distinct 
          name, 
          campaign_id 
        from total_cost
        ) campaign_name 
        on campaign_name.campaign_id = web_campaign.utm_campaign
      where true
        and email is not null

      union all

      --info from appsflyer webflow campaigns
      select 
        week_start_date,
        touchtime,
        ad_platform,
        campaign,
        campaign_id,
        product,
        email,
        event_week_start_date,
        event_first_timestamp
      from app_campaign_merged
      where true
        and lower(campaign) like '%webflow%'
    )
    where true  
      and campaign_id not like '%webflow%' -- just to remove weird lp with weird campaign ids..

    union all

    select
      week_start_date,
      touchtime,
      ad_platform,
      campaign,
      campaign_id,
      product,
      email,
      event_week_start_date,
      event_first_timestamp,
      'app campaign' as source
    from app_campaign_merged
    where true
      and (lower(campaign) not like '%webflow%' or campaign is null)
  )
)

select
  c.week_start_date as "Week Start Date",
  cast(cost.total_cost as decimal(16,2)) as "Cost",
  avg(cost.total_impressions) as "Ad Impressions",
  avg(cost.total_clicks) as "Ad Clicks",
  cast(avg(cost.total_clicks)*1.0/avg(cost.total_impressions) as decimal(16,4)) as "Ad CTR %",  
  cast(count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end)*1.0/avg(cost.total_clicks) as decimal(16,4))as "New SU CVR %",
  count(distinct case when c.week_start_date >= '2020-03-13' and c.product = 'new core' then c.email end) as "New Core SU",
  count(distinct case when c.product = 'roar money' then c.email end) as "RM SU",
  count(distinct case when c.product = 'cbplus' then c.email end) as "CB+ SU",
  count(distinct case when c.product = 'instacash' then c.email end) as "IC SU",
  count(distinct case when c.product = 'investment' then c.email end) as "Wealth SU",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end) as "Total New SU #",
  cast(avg(cost.total_cost)*1.0/nullif(count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end),0) as decimal(16,2)) as "Paid CAC $",
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment') then c.email end)*1.0/nullif(count(distinct case when c.week_start_date >= '2020-03-13' and c.product = 'new core' then c.email end),0) as "Core to SU %",

  count(distinct case when c.product = 'roar money' then fundedrm.email end) as "RM Funded",
  cast(count(distinct case when c.product = 'roar money' then fundedrm.email end)*1.0/nullif(count(distinct case when c.product = 'roar money' then c.email end),0) as decimal(16,4)) as "RM Funded %",
  count(distinct case when c.product = 'cbplus' then cbloan.email end) as "CB+ Funded",
  cast(count(distinct case when c.product = 'cbplus' then cbloan.email end)*1.0/nullif(count(distinct case when c.product = 'cbplus' then c.email end),0) as decimal(16,4)) as "CB+ Funded %",
  count(distinct case when c.product = 'instacash' then takeic.email end) as "Ic Taken",
  cast(count(distinct case when c.product = 'instacash' then takeic.email end)*1.0/nullif(count(distinct case when c.product = 'instacash' then c.email end),0) as decimal(16,4)) as "IC Taken %"
  -- (count(distinct case when c.product = 'cbplus' then cbloan.email end)+count(distinct case when c.product = 'instacash' then takeic.email end)+count(distinct case when c.product = 'roar money' then cbloan.email end)) as "18 New Active Member",
  -- cast(avg(cost.total_cost)*1.0/nullif((count(distinct case when c.product = 'cbplus' then cbloan.email end)+count(distinct case when c.product = 'instacash' then takeic.email end)+count(distinct case when c.product = 'roar money' then cbloan.email end)),0) as decimal(16,2)) as "19 New Active Member CAC"
  
from overall c 
left join (
  select 
    week_start_date, 
    sum(total_cost) as total_cost,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from total_cost
  group by 1
  having sum(total_cost) > 0
) cost 
  on c.week_start_date = cost.week_start_date
left join cb_loan_taken cbloan 
  on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
left join user_first_ic takeic 
  on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
left join rm_earliest_deposit_dates fundedrm 
  on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime

where true 
  and c.last_touch_for_prod = 1
  and c.week_start_date >= current_date - interval '30 days'
  
group by 1,2
order by 1 desc