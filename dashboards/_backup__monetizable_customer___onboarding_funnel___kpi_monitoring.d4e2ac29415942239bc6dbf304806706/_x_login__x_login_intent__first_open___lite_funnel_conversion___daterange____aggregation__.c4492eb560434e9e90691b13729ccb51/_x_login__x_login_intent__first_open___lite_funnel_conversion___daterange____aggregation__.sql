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
        [first_open:aggregation] dt,
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
    group by 1
),
------ calculation ------

final as (
    select
        dt,
        first_open,
        case when first_open = 0 then 0 else (1.00 * landing / first_open) end landing_pct,
        case when first_open = 0 then 0 else (1.00 * signup / first_open) end signup_pct,
        case when first_open = 0 then 0 else (1.00 * lite_event / first_open) end lite_event_pct,
        case when first_open = 0 then 0 else (1.00 * passcode / first_open) end passcode_pct,
        case when first_open = 0 then 0 else (1.00 * core_intro / first_open) end core_intro_pct,
        case when first_open = 0 then 0 else (1.00 * core_dob / first_open) end core_dob_pct,
        case when first_open = 0 then 0 else (1.00 * core_address / first_open) end core_address_pct,
        case when first_open = 0 then 0 else (1.00 * core_phone / first_open) end core_phone_pct,
        case when first_open = 0 then 0 else (1.00 * core_ssn / first_open) end core_ssn_pct,
        case when first_open = 0 then 0 else (1.00 * core_identity / first_open) end core_identity_pct,
        case when first_open = 0 then 0 else (1.00 * core_event / first_open) end core_event_pct,
        case when first_open = 0 then 0 else (1.00 * core / first_open) end core_pct,
        core
    from 
        onboarding_summary
)

select * from final
order by 1 desc