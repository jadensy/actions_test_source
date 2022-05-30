-- no cache
with /*date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end]  end_date
)

,*/ onboarding_web as (
select
  "timestamp" as loading_datetime
  , user_id as user_email
  , context_ip
  , context_page_path
  , page_path
  , anonymous_id
  , ROW_NUMBER() OVER(
            PARTITION BY user_id,
            anonymous_id,
            page_path
            ORDER BY
                timestamp ASC
        ) rn
from
  onboarding_web.page_loaded
--  cross join date_select
where
  page_path in ('SUBMIT_EMAIL','SUBMIT_NAME','CREATE_ACCOUNT')
--   and date("loading_datetime")>=start_date
--   and date("loading_datetime")<=end_date
  and date("timestamp") >=dateadd('days', -[days_filter], '2022-03-15')
  and date("timestamp")  <='2022-03-15'
  )

, page_submit_email as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  , anonymous_id
  from onboarding_web
  where page_path in ('SUBMIT_EMAIL') and rn=1
  )

, page_submit_name as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  , anonymous_id
  from onboarding_web
  where page_path in ('SUBMIT_NAME') and rn=1
  )

, page_create_account as (
  select 
  loading_datetime
  , context_ip
  , user_email
  , page_path
  , anonymous_id
  from onboarding_web 
  where page_path in ('CREATE_ACCOUNT') and rn=1
  )

, lite_session as (
  select 
  "timestamp" as lite_session_timestamp
  , context_ip
  , user_id as user_email
  , anonymous_id
  from onboarding_web.lite_account_created
--  cross join date_select
  where 
  date("lite_session_timestamp") >=dateadd('days', -[days_filter], '2022-03-15')
 --  lite_session_timestamp :: date>=start_date
 --   and lite_session_timestamp :: date<=end_date
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
 --   and first_date<=end_date
)


, combined_count as (
  select 
  count(distinct case when a.page_path='SUBMIT_EMAIL' then a.anonymous_id end) as submit_email
  , count(distinct case when b.page_path='SUBMIT_NAME' then a.anonymous_id end) as submit_name
  , count(distinct case when c.page_path='CREATE_ACCOUNT' then a.anonymous_id end) as create_account
  , count(distinct case when f.email is not null then a.anonymous_id end) as lite
  from page_submit_email a
  left join page_submit_name b on b.anonymous_id=a.anonymous_id and a.loading_datetime<b.loading_datetime
  left join page_create_account c on c.anonymous_id=b.anonymous_id and b.loading_datetime<c.loading_datetime
  left join lite_session e on c.anonymous_id=e.anonymous_id and c.loading_datetime<=e.lite_session_timestamp
  left join lite_user f on e.user_email=f.email  
  )

, raw_data as (
    select 1 as order_key, 'submit_email' as funnel_desc, submit_email as event_count from combined_count union all
    select 2 as order_key, 'submit_name' as funnel_desc, submit_name as event_count from combined_count union all
    select 3 as order_key, 'create_account' as funnel_desc, create_account as event_count from combined_count union all
    select 4 as order_key, 'lite_su' as funnel_desc, lite as event_count from combined_count 
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