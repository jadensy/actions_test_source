with dau as
(select
  [timestamp:day] as day,
  count(distinct user_id) as wau
from prod.screen_viewed
where timestamp > '20181231'
and [timestamp:day] <= [sysdate:day]
group by 1),

referral_screen_viewers as 
(select
  [timestamp:day] as day,
  count(distinct user_id) as referral_screen_viewers
from prod.screen_viewed
where screen = 'ReferralContainer'
and timestamp > '20181231'
and [timestamp:day] <= [sysdate:day]
group by 1),

clicks as
(select
  [timestamp:day] as day,
  count(*) as inapp_share_clicks,
  count(distinct user_id) as unique_referrers
from prod.referral_status
where context_app_name != 'ML.staging'
and timestamp > '20181231'
and referral_source is not null
group by 1),

branch_clicks as
(select
  [timestamp:day] as day,
  count(*) as branch_url_clicks
from prod.branch_link_clicked
where timestamp > '2018-12-31'
and referral_code is not null
group by 1),

referral_signups as
(select
  [statushistory_pending_createdon:day] as day,
  count(distinct refereeemail) as registered_referrals
from ml_public.referral
where statushistory_pending_createdon is not null
group by 1),

referral_signups_users as
(select
  distinct userid, refereeemail
from
  (select
    distinct refereeemail,
    b.userid
  from ml_public.referral a
  left join ml_public.usercollection b on a.refereeemail = b.email
  where statushistory_pending_createdon is not null
  group by 1,2)),

plus_enrollments as
(select
  [statushistory_active_on:day] as day,
  count(distinct refereeemail) as plus_referrals
from ml_public.referral
where statushistory_active_on is not null
group by 1),

plus_start as
(select
  day,
  count(distinct userid) as plus_started
from(
  (select
    [a.timestamp:day] as day,
    b.userid as userid
  from prod.page_loaded a
  left join ml_public.usercollection b on a.user_id = b.email
  where page_path = 'LANDING'
  and b.userid in (select userid from referral_signups_users)
  group by
    1,2)
  union all
  (select
    [a.timestamp:day] as day,
    b.userid as userid
  from prod.screen_viewed a
  left join ml_public.usercollection b on a.user_id = b.email
  where screen IN ('PlusIntro', 'PlusUpgradeIntro')
  and b.userid in (select userid from referral_signups_users)
  group by
    1,2))
group by 1)

,

pokes as
(select
  [timestamp:day] as day,
  count(*) as pokes
from prod.plus_referral_status
where referral_status = 'referralPoked'
group by 1),

highfives as
(select
  [timestamp:day] as day,
  count(*) as highfives
from prod.plus_referral_status
where referral_status = 'referralHighFived'
group by 1)

, core as (
  select [timestamp:day] as day, count(distinct user_id) as core_created
  from prod.core_account_created
  where user_id IN (SELECT DISTINCT refereeemail FROM referral_signups_users)
  group by 1 
)

select
  referral_screen_viewers.day,
  wau as DAU,
  referral_screen_viewers,
--   1.00 * isnull(referral_screen_viewers,0) / wau as conv1,
  unique_referrers,
--   1.00 * isnull(unique_referrers,0) / referral_screen_viewers as conv2,
--  inapp_share_clicks,
--  branch_url_clicks,
  registered_referrals,
--   1.00 * isnull(registered_referrals,0) / unique_referrers as conv3,
--   plus_started,
--   1.00 * isnull(plus_started,0) / registered_referrals as conv4,
--  isnull(prebv_attempts,0) as prebv_attempts,
--  1.00 * isnull(prebv_attempts,0) / registered_referrals as conv_rateX,
  isnull(core_created,0) as core_signups,
  isnull(plus_referrals,0) as plus_referrals
-- ,  1.00 * isnull(plus_referrals,0) / plus_started as conv5
--  1.00 * isnull(plus_referrals,0) / unique_referrers  as k_factor,
--  isnull(pokes,0) as pokes,
--  isnull(highfives,0) as highfives
from referral_screen_viewers
left join dau on referral_screen_viewers.day = dau.day
left join clicks on referral_screen_viewers.day = clicks.day
left join branch_clicks on referral_screen_viewers.day = branch_clicks.day
left join referral_signups on referral_screen_viewers.day = referral_signups.day
left join plus_enrollments on referral_screen_viewers.day = plus_enrollments.day
left join plus_start on referral_screen_viewers.day = plus_start.day
-- left join prebv_attempts on referral_screen_viewers.day = prebv_attempts.day
left join pokes on referral_screen_viewers.day = pokes.day
left join highfives on referral_screen_viewers.day = highfives.day
left join core on core.day = referral_screen_viewers.day
order by 1 desc