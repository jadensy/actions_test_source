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
  , a.page_path
  , a.user_email
  , g.email as core_email
  , g.first_date as core_date
  from page_su i
  left join lite_user h on i.user_email=h.email  
  left join onboarding_web a on a.user_email=h.email and h.first_timestamp<a.loading_datetime 
  left join dob b on a.user_email=b.user_email and a.loading_datetime<b.loading_datetime 
  left join address c on c.user_email=b.user_email and b.loading_datetime<c.loading_datetime 
  left join mobile_phone_number d on d.user_email=c.user_email and c.loading_datetime<d.loading_datetime
  left join ssn e on e.user_email=d.user_email and d.loading_datetime<e.loading_datetime
  left join confirm_info f on e.user_email=f.user_email and e.loading_datetime<f.loading_datetime
  left join core_user g on f.user_email=g.email and f.loading_datetime<g.first_timestamp  
  
  )



, raw_data as (
  select 
  [loading_date:aggregation] as pii_date
  , count(distinct user_email) as pii_count
  , count(distinct core_email) as core_count
  , count(distinct case when core_date-loading_date < 1 then user_email end) D0_cnt
  , count(distinct case when core_date-loading_date <= 1 then user_email end) D1_cnt
  , count(distinct case when core_date-loading_date <= 7 then user_email end) D7_cnt
  , count(distinct case when core_date-loading_date <= 14 then user_email end) D14_cnt
  , count(distinct case when core_date-loading_date <= 30 then user_email end) D30_cnt
  , count(distinct case when core_date-loading_date <= 60 then user_email end) D60_cnt
  , count(distinct case when core_date-loading_date <= 90 then user_email end) D90_cnt
  from combined_count
  group by 1
  )

select 
pii_date as dt
, pii_count
, core_count
, 1.0*D0_cnt/pii_count as D0
, case when date_select.end_date < pii_date + 1 then null 
       else 1.0*D1_cnt/pii_count end as D1
, case when date_select.end_date < pii_date + 7 then null 
       else 1.0*D7_cnt/pii_count end as D7
, case when date_select.end_date < pii_date + 14 then null 
       else 1.0*D14_cnt/pii_count end as D14
, case when date_select.end_date < pii_date + 30 then null 
       else 1.0*D30_cnt/pii_count end as D30
, case when date_select.end_date < pii_date + 60 then null 
       else 1.0*D60_cnt/pii_count end as D60
, case when date_select.end_date < pii_date + 90 then null 
       else 1.0*D90_cnt/pii_count end as D90
, case when date_select.end_date > pii_date + 90 then 1.0*core_count/pii_count 
       else null end as "D90+"
from raw_data
cross join date_select
 where pii_count>0
order by 1 desc