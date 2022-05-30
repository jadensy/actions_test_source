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
  , case when credit_builder_plus='t' then 'cbplus'
    when investment='t' then 'investment standalone'
    when instacash='t' then 'instacash'
    when banking='t' then 'roar money'
    end as product
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
      'new core', 'lite'
    )
    and lower(source) = 'webapp'
    and first_date>=start_date
    and rn=1
)



, combined_count as (
  select 
  date(d."interest_date") as interest_date
  , d.user_email
  , b.email as ffa_email
  , b.first_date as ffa_date
  from interest_in_product d
  left join ffa_user b on d.user_email=b.email and d.interest_date<b.first_timestamp  
  )


, raw_data as (
  select 
  [interest_date:aggregation] as interest_date
  , count(distinct user_email) as interest_count
  , count(distinct ffa_email) as ffa_count
  from combined_count
  group by 1
  
  )

select 
interest_date as dt
, interest_count
, ffa_count
, 1.0*ffa_count/interest_count as interest_to_ffa
from raw_data
cross join date_select
order by 1 desc