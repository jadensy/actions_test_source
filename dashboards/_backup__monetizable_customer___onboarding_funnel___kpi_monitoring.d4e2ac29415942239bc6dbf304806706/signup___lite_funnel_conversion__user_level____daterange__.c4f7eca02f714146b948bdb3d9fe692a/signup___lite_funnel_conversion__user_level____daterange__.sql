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
  --count(distinct case when a.page_path='SUBMIT_EMAIL' then a.user_email end) as submit_email
  count(distinct a.user_email) as signup
  , count(distinct case when d.email is not null then a.user_email end) as lite
  from page_su a
  left join lite_user d on a.user_email=d.email
  )


, raw_data as (
    
    select 1 as order_key, 'signup' as funnel_desc, signup as event_count from combined_count union all
    select 2 as order_key, 'lite_su' as funnel_desc, lite as event_count from combined_count 
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.signup) conversion_pct,
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
                signup
            from 
                combined_count
        ) t2
)


select 
*
from final
order by 1