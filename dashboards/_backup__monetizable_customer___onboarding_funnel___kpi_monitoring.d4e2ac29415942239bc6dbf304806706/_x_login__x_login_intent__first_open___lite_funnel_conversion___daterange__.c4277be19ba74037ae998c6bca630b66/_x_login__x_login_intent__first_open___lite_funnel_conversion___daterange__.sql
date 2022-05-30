/*
MAIN: main script for onboarding funnel
this script has been created into a view in periscope
*/

with date_select AS (
    select
        [daterange_start] start_date,
        [daterange_end] end_date
),

------ lite journey ------

lite_journey as (
    select
        anonymous_id,
        detected_user_email,
        platform,
        first_open,
        landing,
        signup,
        lite_event,
        passcode,
        lite,
        login_screen,
        is_login_intent
    from 
        [lite_onboarding_funnel]
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
        count(lite) lite
    from 
        lite_journey
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
    select 6 as order_key, 'lite' as funnel_desc, lite as event_count from onboarding_summary
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