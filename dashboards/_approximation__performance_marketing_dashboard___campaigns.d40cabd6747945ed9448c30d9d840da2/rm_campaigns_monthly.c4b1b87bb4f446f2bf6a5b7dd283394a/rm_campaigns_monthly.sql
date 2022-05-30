-- This dashboard/chart has been updated by DE to apply the DDA2 schema migration changes on 2020-11-01 03:18:20.170922 
-- For more info, see https://moneylion.atlassian.net/wiki/spaces/DE/pages/1809711844/DDA2+Schema+Migration+NOV+2020

with overall as (
  select *
  from (
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
      and paid_ffa_type is not null

    union all

    select 
      new_core_last_ad_click_dtime as touchtime,
      'new core' as product,
      email,
      userid,
      new_core_channel as ad_platform,
      new_core_campaign_name as campaign,
      new_core_campaign_id as campaign_id,
      new_core_SU_dtime as event_first_timestamp,
      new_core_campaign_type as source
    
    from marketing.fct_pfmkt_user_view_ffa 
    where 1=1
      and new_core_channel is not null
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

total_cost as (
  select
    *,
    [date_stop:week] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
)

select distinct
  coalesce(c.mth, cost.mth) as mth,
  coalesce(c.ad_platform, cost.ad_platform) as ad_platform,
  coalesce(cost.campaign, c.campaign) as campaign,
  coalesce(c.campaign_id, cost.campaign_id) as campaign_id,


  cast(cost.total_cost as decimal(16,2)) as "Cost",
  cost.total_impressions as "Ad Impressions",
  cost.total_clicks as "Ad Clicks",
  cast(cost.total_clicks*1.0/cost.total_impressions as decimal(16,4)) as "Ad CTR %",  
  cast(c."Total New SU" * 1.0/cost.total_clicks as decimal(16,4))as "New SU CVR %",

  c."New Core SU",
  c."RM SU",
  c."CB+ SU",
  c."IC SU",
  c."Wealth SU",
  c."Total New SU",
  cast(cost.total_cost*1.0/nullif(c."Total New SU",0) as decimal(16,2)) as "Paid CAC",
  c."RM Funded",
  c."RM Funded %",
  c."CB+ Funded",
  c."CB+ Funded %",
  c."IC Taken",
  c."IC Taken %"


from (
  select distinct 

    [c.touchtime:month] as mth,
    c.ad_platform,
    c.campaign,
    c.campaign_id,
    count(distinct case when c.touchtime >= '2020-03-13' and c.product = 'new core' then c.email end) as "New Core SU",
    count(distinct case when c.product = 'roar money' then c.email end) as "RM SU",
    count(distinct case when c.product = 'cbplus' then c.email end) as "CB+ SU",
    count(distinct case when c.product = 'instacash' then c.email end) as "IC SU",
    count(distinct case when c.product = 'investment standalone' then c.email end) as "Wealth SU",
    count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end) as "Total New SU",

    count(distinct case when c.product = 'roar money' then fundedrm.email end) as "RM Funded",
    cast(count(distinct case when c.product = 'roar money' then fundedrm.email end)*1.0/nullif(count(distinct case when c.product = 'roar money' then c.email end),0) as decimal(16,4)) as "RM Funded %",
    count(distinct case when c.product = 'cbplus' then cbloan.email end) as "CB+ Funded",
    cast(count(distinct case when c.product = 'cbplus' then cbloan.email end)*1.0/nullif(count(distinct case when c.product = 'cbplus' then c.email end),0) as decimal(16,4)) as "CB+ Funded %",
    count(distinct case when c.product = 'instacash' then takeic.email end) as "IC Taken",
    cast(count(distinct case when c.product = 'instacash' then takeic.email end)*1.0/nullif(count(distinct case when c.product = 'instacash' then c.email end),0) as decimal(16,4)) as "IC Taken %"

  from overall c
  left join cb_loan_taken cbloan 
    on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
  left join user_first_ic takeic 
    on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
  left join rm_earliest_deposit_dates fundedrm 
    on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime
  where 1=1
--     and c.ad_platform in ('facebook', 'google')
--     and source = 'app campaign'
  group by 1,2,3,4
)c 

full outer join (
  select 
    [date_stop:month] as mth,
    name as campaign,
    campaign_id,
    ad_platform,
    sum(total_cost) as total_cost,
    sum(total_clicks) as total_clicks,
    sum(total_impressions) as total_impressions
  from total_cost
  where true
--     and ad_platform in ('facebook', 'google')
--     and lower(campaign) not like '%webflow%'
  group by 1,2,3,4
  having sum(total_cost) > 0
) cost on c.mth = cost.mth and c.ad_platform = cost.ad_platform and c.campaign_id = cost.campaign_id


where 1=1 
  and (lower(coalesce(cost.campaign, c.campaign)) like '%roar%' or lower(coalesce(cost.campaign, c.campaign)) like '%rmwebflow%')

order by 1 desc, 2,3,4,5 asc