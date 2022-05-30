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
  count(distinct case when a.page_path='PII_START' then a.user_email end) as pii_start
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
  )


, raw_data as (
    select 1 as order_key, 'pii_start' as funnel_desc, pii_start as event_count from combined_count union all
    select 2 as order_key, 'dob' as funnel_desc, dob as event_count from combined_count union all
    select 3 as order_key, 'address' as funnel_desc, address as event_count from combined_count union all
    select 4 as order_key, 'mobile_phone_number' as funnel_desc, mobile_phone_number as event_count from combined_count 
union all
    select 5 as order_key, 'ssn' as funnel_desc, ssn as event_count from combined_count 
union all
    select 6 as order_key, 'confirm_information' as funnel_desc, confirm_information as event_count from combined_count 
union all
    select 7 as order_key, 'core' as funnel_desc, core as event_count from combined_count 
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.pii_start) conversion_pct,
        (1.00 * t1.event_count / t1.previous_event_count) previous_step_pct
    from 
        (
            select
                order_key,
                funnel_desc,
                event_count,
                lag(event_count, 1) over(
                    order by 
                        order_key
                ) previous_event_count
            from 
                raw_data
        ) t1
        cross join (
            select 
                pii_start
            from 
                combined_count
        ) t2
)


select 
*
from final
order by 1