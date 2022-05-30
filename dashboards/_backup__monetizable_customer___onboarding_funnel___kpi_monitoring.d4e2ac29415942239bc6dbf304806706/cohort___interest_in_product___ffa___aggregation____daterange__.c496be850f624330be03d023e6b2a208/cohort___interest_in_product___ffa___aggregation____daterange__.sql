-- no cache
with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end] end_date
)


--core user
/*, core_user as (
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
    and first_date<=end_date
)
*/


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


, interest_in_product as (
select
  a.anonymous_id
  , a.user_id as user_email
  , a."timestamp" as interest_date
  , a.context_ip
  , case when credit_builder_plus='t' then 'cbplus'
    when investment='t' then 'investment standalone'
    when instacash='t' then 'instacash'
    when banking='t' then 'roar money'
    end as product
from
  onboarding_web.interest_in_product_selected a
  inner join organic b on a.user_id=b.user_email
cross join date_select
  where "timestamp" :: date >=start_date
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
  date(d."interest_date") as interest_date
  , d.user_email
  , b.email as ffa_email
  , b.first_date as ffa_date
  from interest_in_product d
  left join ffa_user b on d.user_email=b.email and d.interest_date<b.first_timestamp  
  )


, raw_data as (
  select 
  [interest_date:aggregation] as interest_date
  , count(distinct user_email) as interest_count
  , count(distinct ffa_email) as ffa_count
  , count(distinct case when ffa_date-interest_date < 1 then user_email end) D0_cnt
  , count(distinct case when ffa_date-interest_date <= 1 then user_email end) D1_cnt
  , count(distinct case when ffa_date-interest_date <= 7 then user_email end) D7_cnt
  , count(distinct case when ffa_date-interest_date <= 14 then user_email end) D14_cnt
  , count(distinct case when ffa_date-interest_date <= 30 then user_email end) D30_cnt
  , count(distinct case when ffa_date-interest_date <= 60 then user_email end) D60_cnt
  , count(distinct case when ffa_date-interest_date <= 90 then user_email end) D90_cnt
  from combined_count
  group by 1
  
  )

select 
interest_date as dt
, interest_count
, ffa_count
, 1.0*D0_cnt/interest_count as D0
, case when date_select.end_date < interest_date + 1 then null 
       else 1.0*D1_cnt/interest_count end as D1
, case when date_select.end_date < interest_date + 7 then null 
       else 1.0*D7_cnt/interest_count end as D7
, case when date_select.end_date < interest_date + 14 then null 
       else 1.0*D14_cnt/interest_count end as D14
, case when date_select.end_date < interest_date + 30 then null 
       else 1.0*D30_cnt/interest_count end as D30
, case when date_select.end_date < interest_date + 60 then null 
       else 1.0*D60_cnt/interest_count end as D60
, case when date_select.end_date < interest_date + 90 then null 
       else 1.0*D90_cnt/interest_count end as D90
, case when date_select.end_date > interest_date + 90 then 1.0*ffa_count/interest_count
       else null end as "D90+"
from raw_data
cross join date_select
order by 1 desc