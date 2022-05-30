/*
MAIN: main script for onboarding funnel
this script has been created into a view in periscope
*/

with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end] end_date
),

------ core journey ------
user_journey as (
    select
        anonymous_id,
        detected_user_email,
        platform,
        is_login_intent,

        first_open,
        landing,
        signup,
        lite_event,
        passcode,
        lite,
        login_screen,

        core_intro,
        core_dob,
        core_address,
        core_phone,
        core_ssn,
        core_identity,
        core_event,
        core
    from 
        [core_onboarding_funnel]
        cross join date_select
    where
        first_open >= date_select.start_date
        and first_open <= date_select.end_date
),


onboarding_summary as (
    select
        count(first_open) first_open,
        count(landing) landing,
        count(signup) signup,
        count(lite_event) lite_event,
        count(passcode) passcode,
        count(lite) lite,
        count(core_intro) core_intro,
        count(core_dob) core_dob,
        count(core_address) core_address,
        count(core_phone) core_phone,
        count(core_ssn) core_ssn,
        count(core_identity) core_identity,
        count(core_event) core_event,
        count(core) core
    from 
        user_journey
    where
        is_login_intent = 0
),
------ calculation ------

raw_data as (
    select 1 as order_key, 'first_open' as funnel_desc, first_open as event_count from onboarding_summary union all
    select 2 as order_key, 'landing' as funnel_desc, landing as event_count from onboarding_summary union all
    select 3 as order_key, 'signup' as funnel_desc, signup as event_count from onboarding_summary union all
    select 4 as order_key, 'lite_event' as funnel_desc, lite_event as event_count from onboarding_summary union all
    select 5 as order_key, 'passcode' as funnel_desc, passcode as event_count from onboarding_summary union all
    select 6 as order_key, 'lite' as funnel_desc, lite as event_count from onboarding_summary union all 
    select 7 as order_key, 'core_intro' as funnel_desc, core_intro as event_count from onboarding_summary union all
    select 8 as order_key, 'core_dob' as funnel_desc, core_dob as event_count from onboarding_summary union all
    select 9 as order_key, 'core_address' as funnel_desc, core_address as event_count from onboarding_summary union all
    select 10 as order_key, 'core_phone' as funnel_desc, core_phone as event_count from onboarding_summary union all
    select 11 as order_key, 'core_ssn' as funnel_desc, core_ssn as event_count from onboarding_summary union all
    select 12 as order_key, 'core_identity' as funnel_desc, core_identity as event_count from onboarding_summary union all
    select 13 as order_key, 'core_event' as funnel_desc, core_event as event_count from onboarding_summary union all
    select 14 as order_key, 'core' as funnel_desc, core as event_count from onboarding_summary
),

final as (
    select
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,

        (1.00 * t1.event_count / t2.first_open) conversion_pct,
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
                first_open
            from 
                onboarding_summary
        ) t2
)

select * from final
order by 1