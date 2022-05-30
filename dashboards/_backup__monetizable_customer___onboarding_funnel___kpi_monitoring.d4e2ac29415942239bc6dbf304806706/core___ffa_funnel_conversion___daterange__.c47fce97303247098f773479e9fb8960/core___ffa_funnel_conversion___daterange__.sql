-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        ([daterange_end] - interval '1 day') end_date
)

--core user
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
  --  and first_date>=start_date
    and first_date<=end_date
)

, organic as (
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
--  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
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
  inner join lite_user b on a.userid=b.userid and a.first_timestamp>b.first_timestamp
  inner join organic c on a.email=c.user_email and c.loading_datetime<a.first_timestamp
  cross join date_select
  where
    true
    and a.product in (
      'new core'
    )
    and lower(a.source) = 'webapp'
    and a.first_date>=start_date
    and a.first_date<=end_date
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
  page_path in ('VERIFICATION_SUCCESS')
  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
  )


, interest_in_product as (
select
  anonymous_id
  , user_id as user_email
  , "timestamp" as interest_date
  , context_ip
  , case when credit_builder_plus='t' then 'cbplus'
    when investment='t' then 'investment standalone'
    when instacash='t' then 'instacash'
    when banking='t' then 'roar money'
    end as product
from
  onboarding_web.interest_in_product_selected
  cross join date_select
where 
  "timestamp" :: date >=start_date
  and "timestamp" :: date <=end_date
  )

--ffa user
, ffa_user as (
  select
    a.userid
    , a.email
    , a.product
    , a.first_date
    , a.first_timestamp
  from
    marketing.stg_user__membership__merged_with_aff as a
  cross join date_select
  where
    true
    and a.product not in (
      'new core', 'lite'
    )
    and lower(source) = 'webapp'
    and first_date>=start_date
    and rn=1
)


, combined_count as (
  select 
  count(distinct case when a.email is not null then a.email end) as core
  , count(distinct case when e.user_email is not null then a.email end) as user_segmentation
  , count(distinct case when d.user_email is not null then a.email end) as interest_in_product
  , count(distinct case when b.email is not null then a.email end) as ffa
  from core_user a
  left join onboarding_web e on e.user_email=a.email and e.loading_datetime>a.first_timestamp
  left join interest_in_product d on d.anonymous_id=e.anonymous_id and e.loading_datetime<d.interest_date  
  left join ffa_user b on d.user_email=b.email and d.interest_date<b.first_timestamp  
  
  )

, raw_data as (
    select 1 as order_key, 'core' as funnel_desc, core as event_count from combined_count union all
    select 2 as order_key, 'user_segmentation' as funnel_desc, user_segmentation as event_count from combined_count union all 
    select 3 as order_key, 'interest_in_product' as funnel_desc, interest_in_product as event_count from combined_count union all
    select 4 as order_key, 'ffa' as funnel_desc, ffa as event_count from combined_count 
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.core) conversion_pct,
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
                core
            from 
                combined_count
        ) t2
)


select 
*
from final
order by 1