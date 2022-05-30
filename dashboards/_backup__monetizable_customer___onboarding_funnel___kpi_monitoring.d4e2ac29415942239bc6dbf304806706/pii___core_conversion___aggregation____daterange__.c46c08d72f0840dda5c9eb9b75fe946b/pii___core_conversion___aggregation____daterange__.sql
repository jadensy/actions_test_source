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
  , context_page_path
  , page_path
from
  onboarding_web.page_loaded
  cross join date_select
where
  upper(page_path) in ('SUBMIT_NAME','CREATE_ACCOUNT','SIGNUP','PII_START','DOB','ADDRESS', 'MOBILE_PHONE_NUMBER','SSN','CONFIRM_INFORMATION')
  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
  )

, page_su as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web
  where upper(page_path) in ('SUBMIT_NAME', 'CREATE_ACCOUNT', 'SIGNUP')
  )


, dob as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='DOB'
  )

, address as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ADDRESS'
  )

, mobile_phone_number as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='MOBILE_PHONE_NUMBER'
  )

, ssn as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='SSN'
  )

, confirm_info as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='CONFIRM_INFORMATION'
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


, combined_count as (
  select 
  date(a."loading_datetime") as loading_date
  , count(distinct case when a.page_path='PII_START' then a.user_email end) as pii_start
  , count(distinct case when b.page_path='DOB' then a.user_email end) as dob
  , count(distinct case when c.page_path='ADDRESS' then a.user_email end) as address
  , count(distinct case when d.page_path='MOBILE_PHONE_NUMBER' then a.user_email end) as mobile_phone_number
  , count(distinct case when e.page_path='SSN' then a.user_email end) as ssn  
  , count(distinct case when f.page_path='CONFIRM_INFORMATION' then a.user_email end) as confirm_information
  , count(distinct case when g.email is not null then a.user_email end) as core
  from page_su i
  left join lite_user h on i.user_email=h.email 
  left join onboarding_web a on a.user_email=h.email and h.first_timestamp<a.loading_datetime 
  left join dob b on a.user_email=b.user_email and a.loading_datetime<b.loading_datetime 
  left join address c on c.user_email=b.user_email and b.loading_datetime<c.loading_datetime 
  left join mobile_phone_number d on d.user_email=c.user_email and c.loading_datetime<d.loading_datetime
  left join ssn e on e.user_email=d.user_email and d.loading_datetime<e.loading_datetime
  left join confirm_info f on e.user_email=f.user_email and e.loading_datetime<f.loading_datetime
  left join core_user g on f.user_email=g.email and f.loading_datetime<g.first_timestamp  
  group by 1
  )


, final as (
  select 
  [loading_date:aggregation] as loading_date
  , pii_start
  , 1.0*core/pii_start as "PII to core"
  from combined_count
  where pii_start>0
  )


select 
*
from final
order by 1