-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        ([daterange_end] - interval '1 day') end_date
)

/*, onboarding_web as (
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
  upper(page_path) in ('SUBMIT_NAME','CREATE_ACCOUNT','SIGNUP','PII_START','DOB','ADDRESS', 'DOB ADDRESS', 'SSN ESIGNS', 'MOBILE_PHONE_NUMBER','SSN','CONFIRM_INFORMATION')
  and date("loading_datetime")>=start_date
  and date("loading_datetime")<=end_date
  )
*/
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
    and first_date<=end_date
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
    and lower(a.source) = 'webapp'
    and a.first_date>=start_date
    and a.first_date<=end_date
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
    and first_date<=end_date
    and rn=1
)


, final as (
  select 
  [b.first_date:aggregation] dt
  , count(distinct b.email) as core
  , count(distinct case when c.email is not null then b.email end) as ffa
  from core_user b 
  left join ffa_user c on c.email=b.email and c.first_timestamp > b.first_timestamp  
  group by 1
  )


select 
dt,
core,
1.0*ffa/core as core_to_ffa
from final
where core<>0
order by 1