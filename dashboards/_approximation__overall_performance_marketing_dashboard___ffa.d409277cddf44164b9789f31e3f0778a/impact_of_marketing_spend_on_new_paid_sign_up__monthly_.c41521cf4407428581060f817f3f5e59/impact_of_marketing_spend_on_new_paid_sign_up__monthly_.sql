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
    [date_stop:week] as week_start_date
  from [pf_mkt_campaign_level_cost_jump450]
)

select
  date(date_trunc('month', c.touchtime)) as mth,
  cast(cost.total_cost as decimal(16,2)) as total_cost,
  count(distinct case when c.product = 'roar money' then c.email end) as roar_money_count,
  count(distinct case when c.product = 'cbplus' then c.email end) as cbplus_count,
  count(distinct case when c.product = 'instacash' then c.email end) as ic_count,
  count(distinct case when c.product = 'investment standalone' then c.email end) as wealth_count,  
  count(distinct case when c.product in ('instacash','cbplus','roar money','investment standalone') then c.email end) as total_new_su
  
from overall c 
left join (
  select 
    date(date_trunc('month', date_stop)) as mth,
    sum(total_cost) as total_cost
  from total_cost
  group by 1
  having sum(total_cost) > 0
) cost on date(date_trunc('month', c.touchtime)) = cost.mth
-- left join cb_loan_taken cbloan 
--   on cbloan.email = c.email and cbloan.first_timestamp >= c.touchtime
-- left join user_first_ic takeic 
--   on takeic.email = c.email and takeic.first_timestamp >= c.touchtime
-- left join rm_earliest_deposit_dates fundedrm 
--   on fundedrm.email = c.email and fundedrm.first_timestamp >= c.touchtime

where true 
  and c.touchtime >= date('2020-01-01')

group by 1,2
order by 1 asc