WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        ([daterange_end] - INTERVAL '2 day') :: date end_date
),

--------------------------------
-- USER JOURNEY
--------------------------------
------ ONBOARDING JOURNEY ------
first_open AS (
    SELECT
        anonymous_id,
        first_open_date,
        first_open_timestamp,
        rn_first_open
    FROM
        marketing.stg_onboarding_funnel__first_open
        CROSS JOIN date_select
    WHERE
        first_open_date >= date_select.start_date
        AND first_open_date <= date_select.end_date
),

anonid_userid_pair as (

    select
        anonymous_id,
        user_id,
        "timestamp" screen_datetime,
        "timestamp" :: date screen_date,
        row_number() over(
            partition by anonymous_id,
                "timestamp" :: date
            order by
                "timestamp" desc
        ) rn_screen_viewed
    from
        marketing.fct_screen_viewed
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date

),

------ LITE MEMBERSHIP ------
lite_membership AS (
    SELECT
        userid,
        email,
        first_date,
        first_timestamp,
        product,
        product_detail,
        source,
        source_breakdown,
        rn
    FROM
        marketing.stg_user__membership__merged
        CROSS JOIN date_select
    WHERE
        product = 'lite'
        AND rn = 1
),

------ LITE CHECKPOINT JOURNEY ------
lite_checkpoint_journey as (
    select
        anonymous_id,
        landing_date,
        first_name_date,
        passcode_date,
        lite_passcode_email
        
    from
        marketing.stg_onboarding_funnel__lite_checkpoint_journey
),

------ LOGIN SCREEN ------ 
login_screen as (
    select
        anonymous_id,
        user_id,
        screen_date login_date,
        screen_timestamp login_datetime
    from
        marketing.stg_onboarding_funnel__login_screen
        cross join date_select
    where
        screen_date >= date_select.start_date
        and screen_date <= date_select.end_date
),

------ CORE MEMBERSHIP ------
core_membership as (
    SELECT
        userid,
        email,
        first_date,
        first_timestamp,
        product,
        product_detail,
        source,
        source_breakdown,
        rn
    FROM
        marketing.stg_user__membership__merged
        CROSS JOIN date_select
    WHERE
        product = 'new core'
        AND rn = 1
),  

user_journey as (
    select
        first_open.anonymous_id,
        first_open.first_open_date,
        
        anonid_userid_pair.user_id user_email,
        
        lite_membership.userid ml_user_id,
        lite_membership.source lite_source,
        lite_membership.first_date lite_date,
        
        core_membership.first_date core_date,
        
        lite_checkpoint_journey.landing_date,
        lite_checkpoint_journey.first_name_date,
        lite_checkpoint_journey.passcode_date,
        
        login_screen.login_date,
        
        case 
            when lite_date < first_open_date 
            and login_date is not null then 0
            when date_diff('day', lite_date, first_open_date) > 3 then 0
            when (passcode_date is null)
            and login_date is not null then 0
            when lite_source = 'webApp' then 0
            else 1
        end is_firstopen_signup
    
    from 
        first_open
        left join anonid_userid_pair on (
            first_open.anonymous_id = anonid_userid_pair.anonymous_id
            and first_open.first_open_date = anonid_userid_pair.screen_date
            and rn_screen_viewed = 1
        )
        ----- membership ------
        left join lite_membership on 
            lower(anonid_userid_pair.user_id) = lower(lite_membership.email)
        left join core_membership on
            lite_membership.email = core_membership.email
        ------ lite check ------
        left join lite_checkpoint_journey on 
            first_open.anonymous_id = lite_checkpoint_journey.anonymous_id
        left join login_screen on (
            first_open.anonymous_id = login_screen.anonymous_id
            and first_open.first_open_date = login_screen.login_date
        )
)
,

--------------------------------
-- RAW DATA 
--------------------------------

raw_data as (

    select 
        date_trunc('week', first_open_date) :: date dt,
        count(anonymous_id) first_open,
        --count(user_email) login,
        count(lite_date) lite,
        count(core_date) core
        
    
    from user_journey
    where is_firstopen_signup = 1
    group by 1
    order by 1 desc
    
)

select
    
    dt,
    first_open,
    (1.00 * core/ lite) lite_to_core
    

from raw_data
    
limit 100