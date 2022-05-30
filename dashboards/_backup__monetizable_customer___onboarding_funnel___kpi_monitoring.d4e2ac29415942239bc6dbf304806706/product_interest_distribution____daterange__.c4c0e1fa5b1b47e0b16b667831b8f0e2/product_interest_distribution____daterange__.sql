-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end] end_date
)


--core user
/*, core_user as (
  select
    a.userid
    , a.email
    , a.product
    , a.first_date
    , a.first_timestamp
  from
    marketing.stg_user__membership__merged as a
  cross join date_select
  where
    true
    and a.product in (
      'new core'
    )
    and lower(source) = 'webapp'
    and first_date>=start_date
    and first_date<=end_date
)
*/


, organic as (
select
  "timestamp" as loading_datetime
  , user_id as user_email
  , context_ip
  , context_page_path
  , page_path
from
  onboarding_web.page_loaded
  cross join date_select
where
  upper(page_path) in ('SUBMIT_NAME','CREATE_ACCOUNT','SIGNUP','PII_START','DOB','ADDRESS', 'MOBILE_PHONE_NUMBER','SSN','CONFIRM_INFORMATION')
--  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
  )


, interest_in_product as (
select
  a.anonymous_id
  , a.user_id as user_email
  , a."timestamp" as interest_date
  , a.context_ip
  , credit_builder_plus
  , investment
  , instacash
  , banking
  , a.product
from
  onboarding_web.interest_in_product_selected a
  inner join organic b on a.user_id=b.user_email
cross join date_select
  where "timestamp" :: date >=start_date
  and "timestamp" :: date <=end_date
  )

--ffa user
, ffa_user as (
  select
    a.userid
    , a.email
    , a.product
    , a.first_date
    , a.first_timestamp
  from
    marketing.stg_user__membership__merged_with_aff as a
  cross join date_select
  where
    true
    and a.product not in (
      'new core', 'lite','crypto'
    )
    and lower(source) = 'webapp'
    and first_date>=start_date
    and rn=1
)


, combined_count as (
  select 
  b.product as ffa_product
  , d.user_email
  , b.email as ffa_email
  , b.first_date as ffa_date
  , d.product as product_interested
  from interest_in_product d
  left join ffa_user b on d.user_email=b.email and d.interest_date<b.first_timestamp  
  
  )

select 
product_interested
,count(distinct case when user_email is not null then user_email end) as interest_cnt
,count(distinct case when ffa_email is not null then user_email end) as ffa_user_cnt
, 1.0*ffa_user_cnt/interest_cnt as ffa_pct
, count(distinct case when ffa_product='instacash' then user_email end) as ic_ffa
, count(distinct case when ffa_product='cbplus' then user_email end) as cbplus_ffa
, count(distinct case when ffa_product='investment standalone' then user_email end) as inv_ffa
, count(distinct case when ffa_product='roar money' then user_email end) as rm_ffa
, count(distinct case when ffa_product='affiliate' then user_email end) as affiliate
from combined_count
group by 1