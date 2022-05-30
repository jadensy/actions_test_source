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
  date("loading_datetime") as loading_date
  , count(distinct a.user_email) as signup
  , count(distinct case when d.email is not null then a.user_email end) as lite
  from page_su a
  left join lite_user d on a.user_email=d.email
  group by 1
  )

, final as (
  select 
  [loading_date:aggregation] loading_date
  , signup
  , 1.0*lite/signup as name_to_lite
  from combined_count
  )

select * from final