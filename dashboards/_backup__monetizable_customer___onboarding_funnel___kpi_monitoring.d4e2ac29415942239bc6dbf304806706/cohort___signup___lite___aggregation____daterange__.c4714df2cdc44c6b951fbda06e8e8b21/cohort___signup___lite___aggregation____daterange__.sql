-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end]  end_date
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
  page_path in ('SUBMIT_NAME','CREATE_ACCOUNT','SIGNUP')
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
  where page_path in ('SUBMIT_NAME','CREATE_ACCOUNT','SIGNUP')
  )

--lite user
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
  --  and first_date<end_date
)

, combined_count as (
  select 
   date(a."loading_datetime") as loading_date
  , a.page_path
  , a.user_email
  , d.email as lite_email
  , d.first_date as lite_date
  from page_su a
  left join lite_user d on a.user_email=d.email
  )


, raw_data as (
  select 
  [loading_date:aggregation] as signup_date
  , count(distinct user_email) as signup_count
  , count(distinct lite_email) as lite_count
  , count(distinct case when lite_date-loading_date < 1 then user_email end) D0_cnt
  , count(distinct case when lite_date-loading_date <= 1 then user_email end) D1_cnt
  , count(distinct case when lite_date-loading_date <= 7 then user_email end) D7_cnt
  , count(distinct case when lite_date-loading_date <= 14 then user_email end) D14_cnt
  , count(distinct case when lite_date-loading_date <= 30 then user_email end) D30_cnt
  , count(distinct case when lite_date-loading_date <= 60 then user_email end) D60_cnt
  , count(distinct case when lite_date-loading_date <= 90 then user_email end) D90_cnt
  from combined_count
  group by 1
  )

select 
signup_date as dt
, signup_count
, lite_count
, 1.0*D0_cnt/signup_count as D0
, case when date_select.end_date < signup_date + 1 then null 
       else 1.0*D1_cnt/signup_count end as D1
, case when date_select.end_date < signup_date + 7 then null 
       else 1.0*D7_cnt/signup_count end as D7
, case when date_select.end_date < signup_date + 14 then null 
       else 1.0*D14_cnt/signup_count end as D14
, case when date_select.end_date < signup_date + 30 then null 
       else 1.0*D30_cnt/signup_count end as D30
, case when date_select.end_date < signup_date + 60 then null 
       else 1.0*D60_cnt/signup_count end as D60
, case when date_select.end_date < signup_date + 90 then null 
       else 1.0*D90_cnt/signup_count end as D90
, case when date_select.end_date > signup_date + 90 then 1.0*lite_count/signup_count 
       else null end as "D90+"
from raw_data
cross join date_select
order by 1 desc