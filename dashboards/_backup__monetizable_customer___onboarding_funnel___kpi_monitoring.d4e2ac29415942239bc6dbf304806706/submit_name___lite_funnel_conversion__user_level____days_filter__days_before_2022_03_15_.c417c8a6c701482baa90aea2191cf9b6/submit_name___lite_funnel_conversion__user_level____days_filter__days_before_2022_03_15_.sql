-- no cache
with /*date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end]  end_date
)*/

 onboarding_web as (
select
  "timestamp" as loading_datetime
  , user_id as user_email
  , context_ip
  , context_page_path
  , page_path
from
  onboarding_web.page_loaded
where
  page_path in ('SUBMIT_EMAIL','SUBMIT_NAME','CREATE_ACCOUNT')
  and date("timestamp") >=dateadd('days', -[days_filter], '2022-03-15')
  and date("timestamp")  <='2022-03-15'
  )

, page_submit_email as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web
  where page_path in ('SUBMIT_EMAIL')
  )

, page_submit_name as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web
  where page_path in ('SUBMIT_NAME')
  )

, page_create_account as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web 
  where page_path in ('CREATE_ACCOUNT')
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
 -- cross join date_select
  where
    true
    and a.product in (
      'lite'
    )
    and lower(source) = 'webapp'
    and first_date >=dateadd('days', -[days_filter], '2022-03-15')
    
)


, combined_count as (
  select 
   date(a."loading_datetime") as loading_date
  --count(distinct case when a.page_path='SUBMIT_EMAIL' then a.user_email end) as submit_email
  , count(distinct case when a.page_path='SUBMIT_NAME' then a.user_email end) as submit_name
  , count(distinct case when c.page_path='CREATE_ACCOUNT' then a.user_email end) as create_account
  , count(distinct case when d.email is not null then a.user_email end) as lite
  from page_submit_name a
  left join page_create_account c on c.user_email=a.user_email and a.loading_datetime<c.loading_datetime
  left join lite_user d on c.user_email=d.email and c.loading_datetime<d.first_timestamp  
  group by 1
  )


, final as (
  select 
  [loading_date:aggregation] loading_date
  , submit_name
--   , 1.0*submit_name/submit_email as email_to_name
  , 1.0*create_account/submit_name as name_to_create_acct
  , 1.0*lite/submit_name as name_to_lite
  from combined_count
  )

select * from final