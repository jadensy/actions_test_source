-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end] end_date
)

, lite_user as (
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
      'lite'
    )
    and lower(source) = 'webapp'
    and first_date>=start_date
    and first_date<=end_date
  )
  
, onboarding_web as (
select
  "timestamp" as loading_datetime
  , user_id as user_email
  , context_ip
  , anonymous_id
  , page_path
from
  onboarding_web.page_loaded
  cross join date_select
where
  page_path in ('INSTACASH_SUBMIT_EMAIL','INSTACASH_SUBMIT_NAME','INSTACASH_CREATE_ACCOUNT','INSTACASH_PII_START','INSTACASH_DOB','INSTACASH_ADDRESS', 'INSTACASH_MOBILE_PHONE_NUMBER','INSTACASH_SSN','INSTACASH_CONFIRM_INFORMATION')
  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
  )

, page_submit_name as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  , anonymous_id
  from onboarding_web
  where page_path in ('INSTACASH_SUBMIT_NAME')
  )

, page_create_account as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web 
  where page_path in ('INSTACASH_CREATE_ACCOUNT')
  )

, page_pii as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web 
  where page_path in ('INSTACASH_PII_START')
  )

, dob as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='INSTACASH_DOB'
  )

, address as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='INSTACASH_ADDRESS'
  )

, mobile_phone_number as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='INSTACASH_MOBILE_PHONE_NUMBER'
  )

, ssn as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='INSTACASH_SSN'
  )

, confirm_info as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='INSTACASH_CONFIRM_INFORMATION'
  )
  

--core user
, core_user as (
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
)

, ic_ffa_session AS (
    SELECT
        anonymous_id,
        user_id email,
        timestamp :: date ffa_checkpoint_date
    FROM
        onboarding_web.ic_membership_created
        CROSS JOIN date_select
    WHERE
        timestamp :: date >= date_select.start_date
)


, combined_count as (
  select
  date(a."loading_datetime") as loading_date 
  , count(distinct case when a.page_path='INSTACASH_SUBMIT_EMAIL' then a.anonymous_id end) as submit_email
  , count(distinct case when l.email is not null then l.email end) as ic
  from onboarding_web a
  left join page_submit_name b on a.anonymous_id=b.anonymous_id and a.loading_datetime<b.loading_datetime
  left join page_create_account c on b.user_email=c.user_email and b.loading_datetime<c.loading_datetime
  left join lite_user d on c.user_email=d.email and c.loading_datetime<d.first_timestamp 
  left join page_pii e on e.user_email=d.email and d.first_timestamp <e.loading_datetime 
  left join dob f on e.user_email=f.user_email and e.loading_datetime<f.loading_datetime 
  left join address g on f.user_email=g.user_email and f.loading_datetime<g.loading_datetime 
  left join mobile_phone_number h on g.user_email=h.user_email and g.loading_datetime<h.loading_datetime
  left join ssn i on h.user_email=i.user_email and h.loading_datetime<i.loading_datetime
  left join confirm_info j on i.user_email=j.user_email and i.loading_datetime<j.loading_datetime
  left join core_user k on j.user_email=k.email and j.loading_datetime<k.first_timestamp  
  left join ic_ffa_session l on l.email=k.email 
  group by 1
  )

select 
[loading_date:aggregation] as loading_date
, submit_email
, 1.0*ic/submit_email as email_to_IC
from combined_count