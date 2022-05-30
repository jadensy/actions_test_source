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
  --  and first_date<end_date
)


, combined_count as (
  select 
  --count(distinct case when a.page_path='SUBMIT_EMAIL' then a.user_email end) as submit_email
  count(distinct case when a.page_path='SUBMIT_NAME' then a.user_email end) as submit_name
  , count(distinct case when c.page_path='CREATE_ACCOUNT' then a.user_email end) as create_account
  , count(distinct case when d.email is not null then a.user_email end) as lite
  from page_submit_name a
  left join page_create_account c on c.user_email=a.user_email and a.loading_datetime<c.loading_datetime
  left join lite_user d on c.user_email=d.email and c.loading_datetime<d.first_timestamp  
  )


, raw_data as (
    
    select 1 as order_key, 'submit_name' as funnel_desc, submit_name as event_count from combined_count union all
    select 2 as order_key, 'create_account' as funnel_desc, create_account as event_count from combined_count union all
    select 3 as order_key, 'lite_su' as funnel_desc, lite as event_count from combined_count 
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.submit_name) conversion_pct,
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
                submit_name
            from 
                combined_count
        ) t2
)


select 
*
from final
order by 1