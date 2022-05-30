with live_rec_treatment as 
(
  with user_state as (
  select user_id, trim(lower(personal_info__state)) as state
  from usermetrics.usermetrics
  )

  , tx_fl as (
    select distinct user_id
    from user_state
    where lower(state) ~ '((texas)|(florida))' or lower(state) = 'tx' or lower(state) = 'fl'
    )

  , account_age_30 as(
    select _id as user_id
    from lion1.user 
    where datediff(day, createdon, getdate()) > 30
  )

  , live_rec_model_treatment as(
  select user_id
  from marketplace_aide.list_data_marketplacev1realtimestatelist
  where recommender = 'aws_personalize'
  and algorithm = 'arn:aws:personalize:us-east-1:108786681246:campaign/marketplace-recommendations-v1-campaign-20211208'
  )

  , login_user as (
    select distinct _id as user_id
    from (
      select user_id, max(date_login)
        from (select user_id,
                     date("timestamp") as date_login,
                     context_os_name as os_type
                     FROM ios.screen_viewed
              where user_id is not null
              and screen is not null
              and date("timestamp") between '2022-01-01' and '2022-01-28'
              union all
              select user_id,
                     date("timestamp") AS date_login,
                     context_os_name AS os_type
              from android.screen_viewed
              where user_id is not null
              and screen is not null
              and date("timestamp") between '2022-01-01' and '2022-01-28'
              )
        group by 1
        ) as login_user 
    join lion1.user as users
    on login_user.user_id = users.email
  )

  , gameplan_user as(
  select distinct users._id as user_id 
    from (
      select user_id from ios.gp_focus_area_save
      union all
      select user_id from android.gp_focus_area_save
        )as tmp
    join lion1.user as users
    on tmp.user_id = users.email
    where brand = 'ml' and deleted <> 1
  )

  select distinct login_user.user_id
  from login_user
  where login_user.user_id not in (select distinct user_id from tx_fl where user_id is not null) --exclude opploan user
  and login_user.user_id in (select distinct user_id from live_rec_model_treatment where user_id is not null) --include only live rec treatment group
  and login_user.user_id in (select distinct user_id from account_age_30 where user_id is not null) --include only account age > 30
  and login_user.user_id not in (select distinct user_id from gameplan_user where user_id is not null) --exclude gameplan user
  and login_user.user_id is not null
  order by 1
)

, exp_user as
(select user_id, assignment_group, '2022-01-28'::date as first_injection_time
from marketplace_static.exp_rec_orm_20220126 as exp
group by 1,2

union all 

select user_id, 'live_rec_treatment_group' as assignment_group, '2022-01-28'::date as first_injection_time
from live_rec_treatment
)

,partner_rev as (
select partner_service_name, revenue_per_conversion
from marketplace_static.partner_revenue_per_conversion 
where month = '2021-11-01'
)

, engagement as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group'
        else assignment_group end) as assignment_group_name,
      engagement.partner,
      mapping.conversion_partner_name as conversion_partner_name,
      count(case when event = 'view' then email else null end) as total_view,
      count(case when event = 'click' then email else null end) as total_click,
      count(distinct case when event = 'view' then email else null end) as uniq_view,
      count(distinct case when event = 'click' then email else null end) as uniq_click
  from marketplace.fct_organic_engagement_events as engagement
  join exp_user 
  on engagement.userid  = exp_user.user_id and engagement.event_timestamp >= exp_user.first_injection_time
  left join marketplace_static.partner_name_mapping as mapping 
  on engagement.partner = mapping.engagement_partner_title and engagement.event_timestamp between mapping.mapping_start_time and   mapping.mapping_end_time
  where event_timestamp >= '2022-01-28' and event_timestamp < '2022-02-12'
  and engagement.partner!='LoanInATapCard'
  and medium <> 'dashboard_web'
  group by 1,2,3
)

, conversion as (
  select
      (case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' 
        else assignment_group end) as assignment_group_name,
      fct.partner_service_name AS conversion_partner_name,
    count(fct.userid) as total_conv,
    count(distinct fct.userid) as uniq_conv,
    --round(sum(revenue)::float,2) as total_rev
    round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_rev
  from marketplace.fct_conversion_revenue as fct
  join exp_user 
  on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.first_injection_time
  left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
  on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
  where clicked_at >= '2022-01-28' and clicked_at < '2022-02-12'
  and medium = 'marketplace'
  group by 1,2
)

select engagement.assignment_group_name,
  engagement.partner,
  conversion.conversion_partner_name,
  engagement.uniq_view,
  engagement.total_view,
  round((coalesce(total_rev,0)::decimal / uniq_view),4) as "rev/user"
from engagement 
left join conversion on engagement.conversion_partner_name = conversion.conversion_partner_name and engagement.assignment_group_name = conversion.assignment_group_name
where engagement.assignment_group_name != 'control-control (AIDE inject)'
order by 4 DESC