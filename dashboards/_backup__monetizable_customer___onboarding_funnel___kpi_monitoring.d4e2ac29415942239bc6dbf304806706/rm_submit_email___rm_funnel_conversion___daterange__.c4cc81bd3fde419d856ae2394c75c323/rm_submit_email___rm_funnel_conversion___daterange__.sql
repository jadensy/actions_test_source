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
  upper(page_path) in ('ROARMONEY_SUBMIT_EMAIL','ROARMONEY_SUBMIT_NAME','ROARMONEY_CREATE_ACCOUNT','ROARMONEY_PII_START','ROARMONEY_DOB','ROARMONEY_ADDRESS','ROARMONEY_MOBILE_PHONE_NUMBER','ROARMONEY_SSN','ROARMONEY_CONFIRM_INFORMATION')
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
  where page_path in ('ROARMONEY_SUBMIT_NAME')
  
  )

, page_create_account as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web 
  where page_path in ('ROARMONEY_CREATE_ACCOUNT')
  
  )

, page_pii as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  from onboarding_web 
  where page_path in ('ROARMONEY_PII_START')
 
  )

, dob as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ROARMONEY_DOB'
 
  )

, address as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ROARMONEY_ADDRESS'
 
  )

, mobile_phone_number as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ROARMONEY_MOBILE_PHONE_NUMBER'
  
  )

, ssn as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ROARMONEY_SSN'

  )

, confirm_info as (
select
  loading_datetime
  , user_email
  , context_ip
  , page_path
from
  onboarding_web
where page_path='ROARMONEY_CONFIRM_INFORMATION'

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


, rm_ffa_session AS (
    SELECT
        anonymous_id,
        user_id email,
        timestamp :: date ffa_checkpoint_date
    FROM
        onboarding_web.dda_account_created
        CROSS JOIN date_select
    WHERE
        timestamp :: date >= date_select.start_date
)



, combined_count as (
  select
  count(distinct case when a.page_path='ROARMONEY_SUBMIT_EMAIL' then a.anonymous_id end) as submit_email
  , count(distinct case when b.page_path='ROARMONEY_SUBMIT_NAME' then b.user_email end) as submit_name
  , count(distinct case when c.page_path='ROARMONEY_CREATE_ACCOUNT' then c.user_email end) as create_acct
  , count(distinct case when d.email is not null then d.email end) as lite
  , count(distinct case when e.page_path='ROARMONEY_PII_START' then e.user_email end) as pii_start
  , count(distinct case when f.page_path='ROARMONEY_DOB' then f.user_email end) as dob
  , count(distinct case when g.page_path='ROARMONEY_ADDRESS' then g.user_email end) as address
  , count(distinct case when h.page_path='ROARMONEY_MOBILE_PHONE_NUMBER' then h.user_email end) as mobile_phone_number
  , count(distinct case when i.page_path='ROARMONEY_SSN' then i.user_email end) as ssn  
  , count(distinct case when j.page_path='ROARMONEY_CONFIRM_INFORMATION' then j.user_email end) as confirm_information
  , count(distinct case when k.email is not null then k.email end) as core
  , count(distinct case when l.email is not null then l.email end) as rm
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
  left join rm_ffa_session l on l.email=k.email  
  )


, raw_data as (
    select 1 as order_key, 'submit_email' as funnel_desc, submit_email as event_count from combined_count union all
    select 2 as order_key, 'submit_name' as funnel_desc, submit_name as event_count from combined_count union all
    select 3 as order_key, 'create_acct' as funnel_desc, create_acct as event_count from combined_count union all
    select 4 as order_key, 'lite' as funnel_desc, lite as event_count from combined_count union all
    select 5 as order_key, 'pii_start' as funnel_desc, pii_start as event_count from combined_count union all
    select 6 as order_key, 'dob' as funnel_desc, dob as event_count from combined_count union all
    select 7 as order_key, 'address' as funnel_desc, address as event_count from combined_count union all
    select 8 as order_key, 'mobile_phone_number' as funnel_desc, mobile_phone_number as event_count from combined_count 
union all
    select 9 as order_key, 'ssn' as funnel_desc, ssn as event_count from combined_count 
union all
    select 10 as order_key, 'confirm_information' as funnel_desc, confirm_information as event_count from combined_count 
union all
    select 11 as order_key, 'core' as funnel_desc, core as event_count from combined_count 
union all
    select 12 as order_key, 'RM' as funnel_desc, rm as event_count from combined_count 
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.submit_email) conversion_pct,
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
                submit_email
            from 
                combined_count
        ) t2
)


select 
*
from final
order by 1